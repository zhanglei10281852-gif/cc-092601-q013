from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterable

from app.compute.repository import ComputeRepository
from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, PermissionDeniedError, ValidationError
from app.database import get_connection, transaction

DEFAULT_ARTIFACT_ROOT = Path(__file__).resolve().parent.parent / "data" / "artifacts"
ARTIFACT_PURPOSES = {"grid", "log", "checklist", "other"}


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


def _validate_relative_path(relative_path: str) -> None:
    if not relative_path:
        raise ValidationError("制品路径不能为空")
    if "\\" in relative_path or relative_path.startswith("/") or ".." in PurePosixPath(relative_path).parts:
        raise ValidationError("制品路径必须是制品根目录内的安全相对路径", context={"relative_path": relative_path})


class ComputeOperationsService:
    """管理计算模板、配额、任务租约、结果版本、制品元数据和人工干预。"""

    def __init__(
        self,
        connection: sqlite3.Connection | None = None,
        clock: Clock | None = None,
        *,
        artifact_root: str | Path | None = None,
        artifact_retention_days: int | None = None,
        download_grant_seconds: int | None = None,
    ) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = ComputeRepository(self.connection)
        self.artifact_root = Path(artifact_root or os.getenv("TOWNSHIP_ARTIFACT_ROOT", str(DEFAULT_ARTIFACT_ROOT))).expanduser()
        self.artifact_retention_days = artifact_retention_days or int(os.getenv("TOWNSHIP_ARTIFACT_RETENTION_DAYS", "30"))
        self.download_grant_seconds = download_grant_seconds or int(os.getenv("TOWNSHIP_DOWNLOAD_GRANT_SECONDS", "900"))

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
        results = self.repository.result_versions(task_id)
        for item in results:
            item["artifacts"] = self.repository.artifacts_for(task_id, int(item["version"]))
        result["results"] = results
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
        artifacts: Iterable[dict[str, Any]] | None = None,
        receipt_key: str | None = None,
        retention_days: int | None = None,
    ) -> dict[str, Any]:
        manifest = self._normalize_manifest(artifacts or [])
        now_value = self.clock.now()
        now = to_storage(now_value)
        result_digest = digest({"result": result, "metrics": metrics, "artifacts": manifest})
        effective_receipt = receipt_key or f"content-{result_digest}"
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            existing = repository.result_by_receipt(task_id, effective_receipt)
            if existing is not None:
                if existing["result_digest"] != result_digest:
                    raise ConflictError("同一回执键对应了不同的结果内容")
                replay = dict(repository.task_by_id(task_id))
                replay["replayed"] = True
                return replay
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            self._verify_manifest(manifest)
            days = retention_days if retention_days is not None else self.artifact_retention_days
            retention_until = to_storage(now_value + timedelta(days=days))
            version = int(connection.execute("SELECT COALESCE(MAX(version),0)+1 FROM compute_results WHERE task_id=?", (task_id,)).fetchone()[0])
            connection.execute(
                "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,receipt_key,status,retention_until,created_by,created_at) VALUES(?,?,?,?,?,?,'candidate',?,?,?)",
                (task_id, version, json.dumps(result, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), result_digest, effective_receipt, retention_until, worker_id, now),
            )
            for item in manifest:
                repository.add_artifact(task_id=task_id, result_version=version, receipt_key=effective_receipt, now=now, **item)
            connection.execute(
                "UPDATE compute_tasks SET status='succeeded',current_result_version=?,lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (version, now, now, task_id),
            )
            completed = dict(repository.task_by_id(task_id))
            completed["replayed"] = False
            return completed

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
            if task["status"] not in {"failed", "cancelled", "succeeded"}:
                raise ConflictError("只有失败、已取消或已成功任务可以人工重试")
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

    def list_artifacts(self, task_id: int, version: int | None = None) -> list[dict[str, Any]]:
        if self.repository.task_by_id(task_id) is None:
            raise NotFoundError("计算任务不存在")
        return self.repository.artifacts_for(task_id, version)

    def publish_result(self, task_id: int, version: int, actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if repository.task_by_id(task_id) is None:
                raise NotFoundError("计算任务不存在")
            row = repository.result_version(task_id, version)
            if row is None:
                raise NotFoundError("结果版本不存在")
            if row["status"] == "published":
                return self._version_payload(repository, row)
            if row["status"] == "withdrawn":
                raise ConflictError("已撤回的结果版本不能重新发布")
            connection.execute("UPDATE compute_results SET status='published',published_at=? WHERE task_id=? AND version=?", (now, task_id, version))
            after = repository.result_version(task_id, version)
            repository.add_intervention(task_id=task_id, actor=actor, action="publish_result", reason=f"发布结果版本 {version}", before=dict(row), after=dict(after), batch_key="", now=now)
            return self._version_payload(repository, after)

    def withdraw_result(self, task_id: int, version: int, actor: str, reason: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if repository.task_by_id(task_id) is None:
                raise NotFoundError("计算任务不存在")
            row = repository.result_version(task_id, version)
            if row is None:
                raise NotFoundError("结果版本不存在")
            if row["status"] == "withdrawn":
                return self._version_payload(repository, row)
            connection.execute("UPDATE compute_results SET status='withdrawn',withdrawn_at=?,withdraw_reason=? WHERE task_id=? AND version=?", (now, reason[:1000], task_id, version))
            after = repository.result_version(task_id, version)
            repository.add_intervention(task_id=task_id, actor=actor, action="withdraw_result", reason=reason, before=dict(row), after=dict(after), batch_key="", now=now)
            return self._version_payload(repository, after)

    def authorize_download(self, task_id: int, version: int | None, requester: str) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction() as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            chosen = version if version is not None else task["current_result_version"]
            if chosen is None:
                raise NotFoundError("任务还没有可用的结果版本")
            row = repository.result_version(task_id, int(chosen))
            if row is None:
                raise NotFoundError("结果版本不存在")
            if requester != task["requested_by"] and not repository.is_active_admin(requester):
                raise PermissionDeniedError("无权下载该结果版本")
            if row["status"] == "withdrawn":
                raise ConflictError("结果版本已被撤回，无法授权下载")
            retention_until = row["retention_until"] or ""
            if retention_until and retention_until < now:
                raise ConflictError("结果版本已过保留期，无法授权下载")
            return {
                "task_id": task_id,
                "version": int(chosen),
                "status": row["status"],
                "retention_until": retention_until or None,
                "grant_expires_at": to_storage(now_value + timedelta(seconds=self.download_grant_seconds)),
                "artifacts": repository.artifacts_for(task_id, int(chosen)),
            }

    def cleanup_plan(self) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        classes: dict[str, list[dict[str, Any]]] = {"temporary": [], "candidate": [], "published": [], "withdrawn": []}
        for row in self.repository.artifact_cleanup_rows():
            category, deletable, block_reason = self._classify_artifact(row, now)
            classes[category].append({
                "artifact_id": int(row["id"]),
                "task_id": int(row["task_id"]),
                "result_version": int(row["result_version"]),
                "relative_path": row["relative_path"],
                "size_bytes": int(row["size_bytes"]),
                "digest": row["digest"],
                "purpose": row["purpose"],
                "retention_until": row["retention_until"] or None,
                "deletable": deletable,
                "block_reason": block_reason,
            })
        summary = {
            name: {
                "artifacts": len(items),
                "bytes": sum(item["size_bytes"] for item in items),
                "deletable": sum(1 for item in items if item["deletable"]),
            }
            for name, items in classes.items()
        }
        return {"generated_at": now, "artifact_root": str(self.artifact_root), "classes": classes, "summary": summary}

    @staticmethod
    def _classify_artifact(row: sqlite3.Row, now: str) -> tuple[str, bool, str]:
        status = row["result_status"]
        retention_until = row["retention_until"] or ""
        in_retention = not retention_until or retention_until >= now
        if status == "published":
            return "published", False, "已发布结果仍引用该制品"
        if status == "withdrawn":
            if in_retention:
                return "withdrawn", False, "制品仍在保留期内"
            return "withdrawn", True, ""
        is_current = row["current_result_version"] is not None and int(row["result_version"]) == int(row["current_result_version"]) and row["task_status"] not in {"cancelled", "failed"}
        if not is_current:
            return "temporary", True, ""
        if in_retention:
            return "candidate", False, "制品仍在保留期内"
        return "candidate", True, ""

    @staticmethod
    def _version_payload(repository: ComputeRepository, row: sqlite3.Row) -> dict[str, Any]:
        payload = dict(row)
        payload["artifacts"] = repository.artifacts_for(int(payload["task_id"]), int(payload["version"]))
        return payload

    @staticmethod
    def _normalize_manifest(artifacts: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        manifest: list[dict[str, Any]] = []
        seen: set[str] = set()
        for raw in artifacts:
            relative_path = str(raw.get("relative_path", "")).strip()
            purpose = str(raw.get("purpose", "other"))
            size_bytes = raw.get("size_bytes")
            artifact_digest = str(raw.get("digest", "")).lower()
            if purpose not in ARTIFACT_PURPOSES:
                raise ValidationError(f"制品用途不合法：{purpose or '<empty>'}")
            _validate_relative_path(relative_path)
            if not isinstance(size_bytes, int) or isinstance(size_bytes, bool) or size_bytes < 0:
                raise ValidationError("制品大小必须是非负整数", context={"relative_path": relative_path})
            if len(artifact_digest) != 64 or any(char not in "0123456789abcdef" for char in artifact_digest):
                raise ValidationError("制品摘要必须是 64 位十六进制 sha256", context={"relative_path": relative_path})
            if relative_path in seen:
                raise ValidationError("制品清单包含重复路径", context={"relative_path": relative_path})
            seen.add(relative_path)
            manifest.append({"relative_path": relative_path, "size_bytes": size_bytes, "digest": artifact_digest, "purpose": purpose})
        manifest.sort(key=lambda item: item["relative_path"])
        return manifest

    def _verify_manifest(self, manifest: list[dict[str, Any]]) -> None:
        root = self.artifact_root.resolve()
        for item in manifest:
            target = (root / item["relative_path"]).resolve()
            if not target.is_relative_to(root):
                raise ValidationError("制品路径越出制品根目录", context={"relative_path": item["relative_path"]})
            if not target.is_file():
                raise ValidationError("制品文件不存在", context={"relative_path": item["relative_path"]})
            actual_size = target.stat().st_size
            if actual_size != item["size_bytes"]:
                raise ValidationError("制品大小与清单不一致", context={"relative_path": item["relative_path"], "declared": item["size_bytes"], "actual": actual_size})
            actual_digest = hashlib.sha256(target.read_bytes()).hexdigest()
            if actual_digest != item["digest"]:
                raise ValidationError("制品摘要与清单不一致", context={"relative_path": item["relative_path"]})

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
