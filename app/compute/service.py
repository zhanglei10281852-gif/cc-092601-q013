from __future__ import annotations

import base64
import hashlib
import hmac
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from app.compute.artifacts import ArtifactStore, normalize_relative_path
from app.compute.repository import ComputeRepository
from app.core.clock import Clock, SystemClock, to_storage
from app.core.config import Settings
from app.core.errors import ConflictError, NotFoundError, PermissionDeniedError, ValidationError
from app.database import get_connection, transaction

DOWNLOAD_TOKEN_TTL_SECONDS = 300
TEMPORARY_ARTIFACT_RETENTION_DAYS = 7
# 未配置下载密钥时仅用于本地验收，生产部署必须通过 TOWNSHIP_ARTIFACT_DOWNLOAD_SECRET 覆盖。
LOCAL_DEV_DOWNLOAD_SECRET = "local-dev-artifact-download-secret"


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


class ComputeOperationsService:
    """管理计算模板、配额、任务租约、结果版本、制品与人工干预。"""

    def __init__(
        self,
        connection: sqlite3.Connection | None = None,
        clock: Clock | None = None,
        *,
        artifact_store: ArtifactStore | None = None,
        settings: Settings | None = None,
    ) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = ComputeRepository(self.connection)
        self.settings = settings or Settings.load()
        self.artifact_store = artifact_store or ArtifactStore(self.settings.artifact_storage_root)

    def list_templates(self) -> list[dict[str, Any]]:
        return self.repository.active_templates()

    def create_template(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        self._validate_schema(payload["parameter_schema"], payload["default_parameters"])
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if repository.template_by_code(payload["code"]):
                raise ConflictError("参数模板编码已存在")
            return repository.create_template(
                code=payload["code"], name=payload["name"], algorithm=payload["algorithm"],
                parameter_schema=payload["parameter_schema"], defaults=payload["default_parameters"],
                max_runtime_seconds=payload["max_runtime_seconds"], max_attempts=payload["max_attempts"],
                created_by=actor, now=now,
            )

    def set_quota(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            return ComputeRepository(connection).upsert_quota(actor=actor, now=now, **payload)

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            template = repository.template_by_code(payload["template_code"])
            if template is None or not template["active"]:
                raise NotFoundError("参数模板不存在或已经停用")
            parameters = self._validate_parameters(template, payload["parameters"])
            existing = repository.task_by_idempotency(payload["requested_by"], payload["idempotency_key"])
            parameter_digest = digest(parameters)
            if existing is not None:
                if existing["parameter_digest"] != parameter_digest:
                    raise ConflictError("同一幂等键对应了不同的计算参数")
                return dict(repository.task_by_id(existing["id"]))
            self._check_quota(repository, payload["requested_by"], now_value)
            return repository.create_task(
                template_id=template["id"], project_code=payload["project_code"],
                requested_by=payload["requested_by"], parameters=parameters,
                parameter_digest=parameter_digest, priority=payload["priority"],
                idempotency_key=payload["idempotency_key"], max_attempts=template["max_attempts"], now=now,
            )

    def list_tasks(self, *, status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_tasks(status=status, project_code=project_code, requested_by=requested_by, limit=max(1, min(limit, 500)))

    def get_task(self, task_id: int) -> dict[str, Any]:
        row = self.repository.task_by_id(task_id)
        if row is None:
            raise NotFoundError("计算任务不存在")
        result = dict(row)
        versions = self.repository.result_versions(task_id)
        for version in versions:
            version["artifacts"] = self.repository.artifacts(task_id, version=version["version"])
        result["results"] = versions
        result["interventions"] = self.repository.interventions(task_id)
        return result

    def claim(self, worker_id: str, capabilities: list[str], lease_seconds: int) -> dict[str, Any] | None:
        now_value = self.clock.now()
        now = to_storage(now_value)
        lease_until = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            candidate = repository.queued_candidate(capabilities, now)
            if candidate is None:
                return None
            cursor = connection.execute(
                "UPDATE compute_tasks SET status='running',attempt_count=attempt_count+1,lease_owner=?,lease_expires_at=?,started_at=COALESCE(started_at,?),updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                (worker_id, lease_until, now, now, candidate["id"]),
            )
            if cursor.rowcount != 1:
                return None
            return dict(repository.task_by_id(candidate["id"]))

    def heartbeat(self, task_id: int, worker_id: str, lease_seconds: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            cursor = connection.execute(
                "UPDATE compute_tasks SET lease_expires_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running' AND lease_owner=?",
                (expires, now, task_id, worker_id),
            )
            if cursor.rowcount != 1:
                raise ConflictError("任务未由当前工作者持有")
            return dict(ComputeRepository(connection).task_by_id(task_id))

    def complete(
        self,
        task_id: int,
        worker_id: str,
        result: dict[str, Any],
        metrics: dict[str, Any],
        *,
        completion_receipt: str = "",
        artifacts: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        manifests = artifacts or []
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            # 重复回执必须幂等：不允许再产生结果版本或制品记录。
            if completion_receipt:
                existing = repository.result_by_receipt(task_id, completion_receipt)
                if existing is not None:
                    if existing["created_by"] != worker_id:
                        raise ConflictError("完成回执已由其他工作者提交")
                    return dict(repository.task_by_id(task_id))
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            verified_manifests = self._verify_manifest(repository, task_id, manifests)
            version = int(connection.execute("SELECT COALESCE(MAX(version),0)+1 FROM compute_results WHERE task_id=?", (task_id,)).fetchone()[0])
            repository.create_result(
                task_id=task_id,
                version=version,
                result=result,
                metrics=metrics,
                result_digest=digest({"result": result, "metrics": metrics}),
                completion_receipt=completion_receipt,
                created_by=worker_id,
                now=now,
            )
            default_retain = to_storage(now_value + timedelta(days=self.settings.artifact_retention_days))
            temporary_retain = to_storage(now_value + timedelta(days=TEMPORARY_ARTIFACT_RETENTION_DAYS))
            for item, verified in verified_manifests:
                repository.add_artifact(
                    task_id=task_id,
                    result_version=version,
                    relative_path=item["relative_path"],
                    size_bytes=verified.size_bytes,
                    sha256=verified.sha256,
                    summary=item["summary"],
                    purpose=item["purpose"],
                    role=item["role"],
                    retain_until=default_retain if item["role"] == "permanent" else temporary_retain,
                    created_by=worker_id,
                    now=now,
                )
            connection.execute(
                "UPDATE compute_tasks SET status='succeeded',current_result_version=?,lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (version, now, now, task_id),
            )
            return dict(repository.task_by_id(task_id))

    def _verify_manifest(self, repository: ComputeRepository, task_id: int, manifests: list[dict[str, Any]]) -> list[tuple[dict[str, Any], Any]]:
        seen: set[str] = set()
        verified: list[tuple[dict[str, Any], Any]] = []
        for item in manifests:
            path = normalize_relative_path(item["relative_path"])
            if path in seen:
                raise ValidationError(f"制品清单中存在重复路径：{path}")
            seen.add(path)
            checked = self.artifact_store.verify(path, expected_size=item["size_bytes"], expected_sha256=item["sha256"], task_id=task_id)
            # 相对路径在同一任务下可跨结果版本复用，但物理路径不可变：
            # 已有记录的大小或摘要与本次不同时拒绝，避免新版本覆盖旧版本引用的文件。
            for previous in repository.artifacts(task_id):
                if previous["relative_path"] == path and (previous["sha256"] != checked.sha256 or int(previous["size_bytes"]) != checked.size_bytes):
                    raise ConflictError(f"制品路径已绑定不同内容，不能覆盖：{path}")
            normalized = dict(item)
            normalized["relative_path"] = path
            verified.append((normalized, checked))
        return verified

    def list_artifacts(self, task_id: int, *, version: int | None = None) -> list[dict[str, Any]]:
        if self.repository.task_by_id(task_id) is None:
            raise NotFoundError("计算任务不存在")
        return self.repository.artifacts(task_id, version=version)

    def publish_result_version(self, task_id: int, version: int, actor: str, reason: str) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        retain_until = to_storage(now_value + timedelta(days=self.settings.artifact_retention_days))
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            result = repository.result_version(task_id, version)
            if result is None:
                raise NotFoundError("结果版本不存在")
            if result["revoked_at"]:
                raise ConflictError("结果版本已撤回，不能发布")
            before = dict(result)
            # 首次发布会记录发布人和理由；对已发布版本重复调用则幂等延长保留期。
            repository.publish_result(task_id, version, actor=actor, reason=reason, now=now, retain_until=retain_until)
            after = dict(repository.result_version(task_id, version))
            artifacts_after = repository.artifacts(task_id, version=version)
            repository.add_intervention(
                task_id=task_id, actor=actor, action="result_publish", reason=reason,
                before=before, after={"result": after, "artifacts": artifacts_after}, batch_key="", now=now,
            )
            return {"result": after, "artifacts": artifacts_after}

    def revoke_result_version(self, task_id: int, version: int, actor: str, reason: str, artifact_ids: list[int] | None = None) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            result = repository.result_version(task_id, version)
            if result is None:
                raise NotFoundError("结果版本不存在")
            before = {"result": dict(result), "artifacts": repository.artifacts(task_id, version=version)}
            reason = reason[:1000]
            if artifact_ids:
                owned = {item["id"] for item in repository.artifacts(task_id, version=version)}
                unknown = [identifier for identifier in artifact_ids if identifier not in owned]
                if unknown:
                    raise ValidationError("部分制品不属于该结果版本", context={"artifact_ids": unknown})
                repository.revoke_artifacts(task_id, version, artifact_ids, reason=reason, now=now)
            else:
                if result["revoked_at"]:
                    return {"result": dict(result), "artifacts": repository.artifacts(task_id, version=version)}
                repository.revoke_result(task_id, version, actor=actor, reason=reason, now=now)
            after = {"result": dict(repository.result_version(task_id, version)), "artifacts": repository.artifacts(task_id, version=version)}
            action = "artifact_revoke" if artifact_ids else "result_revoke"
            repository.add_intervention(
                task_id=task_id, actor=actor, action=action, reason=reason,
                before=before, after=after, batch_key="", now=now,
            )
            return after

    def authorize_download(self, artifact_id: int, requester: str) -> dict[str, Any]:
        now_value = self.clock.now()
        artifact = self.repository.artifact_by_id(artifact_id)
        if artifact is None:
            raise NotFoundError("结果制品不存在")
        task = self.repository.task_by_id(artifact["task_id"])
        result = self.repository.result_version(artifact["task_id"], artifact["result_version"])
        if artifact["status"] != "published" or result is None or not result["published_at"] or result["revoked_at"]:
            raise ConflictError("制品尚未发布或其结果版本已撤回")
        if not artifact["retain_until"] or artifact["retain_until"] <= to_storage(now_value):
            raise ConflictError("制品已超过保留期，不能下载")
        if not self._can_access_task(task, requester):
            raise PermissionDeniedError("无权下载该任务的结果制品")
        expires_at = now_value + timedelta(seconds=DOWNLOAD_TOKEN_TTL_SECONDS)
        token = self._sign_download_token(artifact_id, int(expires_at.timestamp()))
        return {
            "artifact_id": artifact_id,
            "task_id": artifact["task_id"],
            "result_version": artifact["result_version"],
            "relative_path": artifact["relative_path"],
            "purpose": artifact["purpose"],
            "size_bytes": artifact["size_bytes"],
            "sha256": artifact["sha256"],
            "download_url": f"/api/compute/artifacts/{artifact_id}/download?token={token}",
            "expires_at": to_storage(expires_at),
        }

    def resolve_download(self, artifact_id: int, token: str) -> tuple[dict[str, Any], Path]:
        now_value = self.clock.now()
        if not self._verify_download_token(artifact_id, token, int(now_value.timestamp())):
            raise PermissionDeniedError("下载令牌无效或已过期")
        artifact = self.repository.artifact_by_id(artifact_id)
        if artifact is None:
            raise NotFoundError("结果制品不存在")
        result = self.repository.result_version(artifact["task_id"], artifact["result_version"])
        if (
            artifact["status"] != "published"
            or result is None
            or not result["published_at"]
            or result["revoked_at"]
            or not artifact["retain_until"]
            or artifact["retain_until"] <= to_storage(now_value)
        ):
            raise ConflictError("制品已不可下载")
        path = self.artifact_store.resolve(artifact["relative_path"], artifact["task_id"])
        if not path.is_file() or path.is_symlink():
            raise NotFoundError("制品文件在存储中不可用")
        return dict(artifact), path

    def cleanup_plan(self) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        categories: dict[str, list[dict[str, Any]]] = {"temporary": [], "candidate": [], "published": [], "revoked": []}
        blocked: list[dict[str, Any]] = []
        for artifact in self.repository.all_artifacts():
            if artifact["status"] == "deleted":
                continue
            item = dict(artifact)
            within_retention = bool(artifact["retain_until"]) and artifact["retain_until"] > now
            referenced = self.repository.path_referenced_by_published_result(artifact["task_id"], artifact["relative_path"])
            reasons: list[str] = []
            if artifact["status"] == "published":
                category = "published"
                eligible = False
                reasons.append("published_artifact_must_be_revoked_first")
                if within_retention:
                    reasons.append("within_retention")
                if referenced:
                    reasons.append("referenced_by_published_result")
            elif artifact["status"] == "revoked":
                category = "revoked"
                # 撤回制品在保留期内留作追溯；保留期过后仍须确认没有其他已发布版本引用同一物理文件。
                eligible = not within_retention and not referenced
                if within_retention:
                    reasons.append("within_retention")
                if referenced:
                    reasons.append("referenced_by_published_result")
            elif artifact["role"] == "temporary":
                category = "temporary"
                eligible = not within_retention and not referenced
                if within_retention:
                    reasons.append("within_retention")
                if referenced:
                    reasons.append("referenced_by_published_result")
            else:
                category = "candidate"
                eligible = not within_retention and not referenced
                if within_retention:
                    reasons.append("within_retention")
                if referenced:
                    reasons.append("referenced_by_published_result")
            item["eligible"] = eligible
            item["reasons"] = reasons
            categories[category].append(item)
        # 同一物理路径可能被多个结果版本的记录共享；只要还有一条记录需要保留，
        # 其他记录即便自身超期也不能删除物理文件，记录状态一并保持。
        all_items = [item for group in categories.values() for item in group]
        protected_paths = {(item["task_id"], item["relative_path"]) for item in all_items if not item["eligible"]}
        blocked: list[dict[str, Any]] = []
        for item in all_items:
            key = (item["task_id"], item["relative_path"])
            if item["eligible"] and key in protected_paths:
                item["eligible"] = False
                item["reasons"].append("shared_path_still_retained")
            if not item["eligible"] and (
                item["status"] in {"published", "revoked"}
                or "referenced_by_published_result" in item["reasons"]
                or "shared_path_still_retained" in item["reasons"]
            ):
                blocked.append(item)
        return {
            "now": now,
            "temporary": categories["temporary"],
            "candidate": categories["candidate"],
            "published": categories["published"],
            "revoked": categories["revoked"],
            "blocked": blocked,
            "eligible_artifact_ids": [item["id"] for group in categories.values() for item in group if item["eligible"]],
        }

    def execute_cleanup(self, actor: str = "cleanup-worker") -> dict[str, Any]:
        plan = self.cleanup_plan()
        now = to_storage(self.clock.now())
        deleted: list[dict[str, Any]] = []
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            for group in ("temporary", "candidate", "revoked"):
                for item in plan[group]:
                    if not item["eligible"]:
                        continue
                    artifact = repository.artifact_by_id(item["id"])
                    if artifact is None or artifact["status"] == "deleted":
                        continue
                    # 最后一道引用保护：仍有已发布结果版本引用同一物理路径时绝不删除。
                    if repository.path_referenced_by_published_result(artifact["task_id"], artifact["relative_path"], exclude_artifact_id=artifact["id"]):
                        continue
                    # 同一物理路径还有处于保留期的其他记录时同样保留。
                    if repository.path_has_live_sibling(artifact["task_id"], artifact["relative_path"], exclude_artifact_id=artifact["id"], now=now):
                        continue
                    path = self.artifact_store.resolve(artifact["relative_path"], artifact["task_id"])
                    file_state = "removed"
                    try:
                        if path.is_symlink():
                            file_state = "symlink_skipped"
                        elif path.exists():
                            path.unlink()
                        else:
                            file_state = "already_absent"
                    except FileNotFoundError:
                        file_state = "already_absent"
                    if file_state != "symlink_skipped":
                        repository.set_artifact_status(artifact["id"], status="deleted", now=now)
                        repository.add_intervention(
                            task_id=artifact["task_id"], actor=actor, action="artifact_cleanup",
                            reason="清理计划删除超过保留期且无引用的制品",
                            before=dict(artifact),
                            after={"id": artifact["id"], "status": "deleted", "file_state": file_state},
                            batch_key="", now=now,
                        )
                        deleted.append({"id": artifact["id"], "relative_path": artifact["relative_path"], "file_state": file_state})
        return {"now": now, "deleted": deleted, "blocked": plan["blocked"]}

    def _can_access_task(self, task: sqlite3.Row | None, requester: str) -> bool:
        if task is not None and task["requested_by"] == requester:
            return True
        row = self.connection.execute(
            "SELECT 1 FROM users u JOIN user_roles ur ON ur.user_id=u.id JOIN roles r ON r.id=ur.role_id WHERE u.username=? AND r.code='administrator' LIMIT 1",
            (requester,),
        ).fetchone()
        return row is not None

    def _download_secret(self) -> bytes:
        secret = self.settings.artifact_download_secret or LOCAL_DEV_DOWNLOAD_SECRET
        return secret.encode()

    def _sign_download_token(self, artifact_id: int, expires_epoch: int) -> str:
        payload = f"{artifact_id}.{expires_epoch}".encode()
        signature = hmac.new(self._download_secret(), payload, hashlib.sha256).hexdigest()
        return base64.urlsafe_b64encode(payload + b"." + signature.encode()).decode()

    def _verify_download_token(self, artifact_id: int, token: str, now_epoch: int) -> bool:
        try:
            raw = base64.urlsafe_b64decode(token.encode()).decode()
            payload_text, signature = raw.rsplit(".", 1)
            signed_artifact, expires_text = payload_text.split(".")
            if int(signed_artifact) != artifact_id:
                return False
            if int(expires_text) < now_epoch:
                return False
            expected = hmac.new(self._download_secret(), payload_text.encode(), hashlib.sha256).hexdigest()
            return hmac.compare_digest(expected, signature)
        except (ValueError, UnicodeDecodeError):
            return False

    def fail(self, task_id: int, worker_id: str, error_code: str, message: str, retryable: bool) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            can_retry = retryable and int(task["attempt_count"]) < int(task["max_attempts"])
            status = "queued" if can_retry else "failed"
            delay = min(300, 2 ** max(0, int(task["attempt_count"]) - 1)) if can_retry else 0
            available = to_storage(now_value + timedelta(seconds=delay))
            connection.execute(
                "UPDATE compute_tasks SET status=?,available_at=?,lease_owner='',lease_expires_at='',last_error_code=?,last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (status, available, error_code, message[:2000], None if can_retry else now, now, task_id),
            )
            return dict(repository.task_by_id(task_id))

    def cancel(self, task_id: int, actor: str, reason: str, batch_key: str = "") -> dict[str, Any]:
        return self._intervene(task_id, actor, reason, "cancel", batch_key, self._cancel_mutation)

    def retry(self, task_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"failed", "cancelled"}:
                raise ConflictError("只有失败或已取消任务可以人工重试")
            chosen = task["priority"] if priority is None else priority
            connection.execute("UPDATE compute_tasks SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?", (chosen, now, now, task["id"]))
        return self._intervene(task_id, actor, reason, "retry", batch_key, mutate)

    def set_priority(self, task_id: int, actor: str, reason: str, priority: int, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"queued", "running"}:
                raise ConflictError("只有排队或运行中的任务可以调整优先级")
            connection.execute("UPDATE compute_tasks SET priority=?,updated_at=?,version=version+1 WHERE id=?", (priority, now, task["id"]))
        return self._intervene(task_id, actor, reason, "priority", batch_key, mutate)

    def batch_operation(self, payload: dict[str, Any]) -> dict[str, Any]:
        batch_key = digest({"actor": payload["actor"], "task_ids": payload["task_ids"], "operation": payload["operation"], "reason": payload["reason"]})
        succeeded: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for task_id in list(dict.fromkeys(payload["task_ids"])):
            try:
                if payload["operation"] == "cancel":
                    value = self.cancel(task_id, payload["actor"], payload["reason"], batch_key)
                elif payload["operation"] == "retry":
                    value = self.retry(task_id, payload["actor"], payload["reason"], payload.get("priority"), batch_key)
                else:
                    value = self.set_priority(task_id, payload["actor"], payload["reason"], int(payload["priority"]), batch_key)
                succeeded.append({"task_id": task_id, "status": value["status"], "version": value["version"]})
            except (ConflictError, NotFoundError) as exc:
                failed.append({"task_id": task_id, "code": exc.code, "message": exc.message})
        return {"batch_key": batch_key, "succeeded": succeeded, "failed": failed}

    def recover_expired(self, actor: str = "recovery-worker") -> dict[str, Any]:
        now = to_storage(self.clock.now())
        recovered: list[int] = []
        exhausted: list[int] = []
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            rows = connection.execute("SELECT * FROM compute_tasks WHERE status='running' AND lease_expires_at<>'' AND lease_expires_at<? ORDER BY id", (now,)).fetchall()
            for task in rows:
                before = dict(task)
                if int(task["attempt_count"]) < int(task["max_attempts"]):
                    status, finished_at = "queued", None
                    recovered.append(int(task["id"]))
                else:
                    status, finished_at = "failed", now
                    exhausted.append(int(task["id"]))
                connection.execute(
                    "UPDATE compute_tasks SET status=?,lease_owner='',lease_expires_at='',available_at=?,last_error_code='lease_expired',last_error_message='工作者租约已过期',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (status, now, finished_at, now, task["id"]),
                )
                after = dict(repository.task_by_id(task["id"]))
                repository.add_intervention(task_id=task["id"], actor=actor, action="lease_recovery", reason="租约过期自动恢复", before=before, after=after, batch_key="", now=now)
        return {"recovered": recovered, "exhausted": exhausted}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM compute_tasks WHERE status='queued'").fetchone()[0]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "templates": len(self.repository.active_templates())}

    def _intervene(self, task_id: int, actor: str, reason: str, action: str, batch_key: str, mutation: Callable[[sqlite3.Connection, sqlite3.Row, str], None]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            before = dict(task)
            mutation(connection, task, now)
            after = dict(repository.task_by_id(task_id))
            repository.add_intervention(task_id=task_id, actor=actor, action=action, reason=reason, before=before, after=after, batch_key=batch_key, now=now)
            return after

    @staticmethod
    def _cancel_mutation(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
        if task["status"] not in {"queued", "running"}:
            raise ConflictError("当前任务状态不允许取消")
        status = "cancel_requested" if task["status"] == "running" else "cancelled"
        connection.execute("UPDATE compute_tasks SET status=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?", (status, None if status == "cancel_requested" else now, now, task["id"]))

    def _check_quota(self, repository: ComputeRepository, requested_by: str, now: datetime) -> None:
        quota = repository.quota("user", requested_by)
        if quota is None:
            return
        states = repository.count_user_states(requested_by)
        if states.get("queued", 0) >= int(quota["max_queued"]):
            raise ConflictError("用户排队任务配额已用尽")
        if states.get("running", 0) >= int(quota["max_running"]):
            raise ConflictError("用户运行任务配额已用尽")
        day_start = to_storage(now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0))
        if repository.count_user_submissions_since(requested_by, day_start) >= int(quota["daily_submissions"]):
            raise ConflictError("用户当日提交配额已用尽")

    @staticmethod
    def _validate_schema(schema: dict[str, dict[str, Any]], defaults: dict[str, Any]) -> None:
        if not schema:
            raise ValidationError("参数模板至少包含一个参数")
        allowed = {"integer", "number", "string", "boolean"}
        for name, rule in schema.items():
            if not name or not isinstance(rule, dict) or rule.get("type") not in allowed:
                raise ValidationError(f"参数 {name or '<empty>'} 的规则不合法")
        if set(defaults) - set(schema):
            raise ValidationError("默认值包含未声明参数")

    def _validate_parameters(self, template: sqlite3.Row, supplied: dict[str, Any]) -> dict[str, Any]:
        schema = json.loads(template["parameter_schema_json"])
        values = {**json.loads(template["default_parameters_json"]), **supplied}
        unknown = set(values) - set(schema)
        if unknown:
            raise ValidationError("包含模板未声明的参数", context={"parameters": sorted(unknown)})
        normalized: dict[str, Any] = {}
        for name, rule in schema.items():
            if name not in values:
                if rule.get("required"):
                    raise ValidationError(f"缺少必填参数：{name}")
                continue
            value = values[name]
            kind = rule["type"]
            valid = {"integer": isinstance(value, int) and not isinstance(value, bool), "number": isinstance(value, (int, float)) and not isinstance(value, bool), "string": isinstance(value, str), "boolean": isinstance(value, bool)}[kind]
            if not valid:
                raise ValidationError(f"参数 {name} 类型不正确")
            if rule.get("minimum") is not None and value < rule["minimum"]:
                raise ValidationError(f"参数 {name} 小于允许的最小值")
            if rule.get("maximum") is not None and value > rule["maximum"]:
                raise ValidationError(f"参数 {name} 大于允许的最大值")
            if rule.get("choices") and value not in rule["choices"]:
                raise ValidationError(f"参数 {name} 不在允许的选项中")
            normalized[name] = value
        return normalized
