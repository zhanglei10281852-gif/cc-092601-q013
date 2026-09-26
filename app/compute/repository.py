from __future__ import annotations

import json
import sqlite3
from typing import Any, Iterable


class ComputeRepository:
    """封装计算任务运营领域的 SQLite 读写。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def template_by_code(self, code: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_templates WHERE code=?", (code,)).fetchone()

    def template_by_id(self, template_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_templates WHERE id=?", (template_id,)).fetchone()

    def active_templates(self) -> list[dict[str, Any]]:
        rows = self.connection.execute("SELECT * FROM compute_templates WHERE active=1 ORDER BY code,version").fetchall()
        return [dict(row) for row in rows]

    def create_template(self, *, code: str, name: str, algorithm: str, parameter_schema: dict[str, Any], defaults: dict[str, Any], max_runtime_seconds: int, max_attempts: int, created_by: str, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO compute_templates(code,name,algorithm,version,parameter_schema_json,default_parameters_json,max_runtime_seconds,max_attempts,active,created_by,created_at,updated_at) VALUES(?,?,?,1,?,?,?,?,1,?,?,?)",
            (code, name, algorithm, json.dumps(parameter_schema, ensure_ascii=False, sort_keys=True), json.dumps(defaults, ensure_ascii=False, sort_keys=True), max_runtime_seconds, max_attempts, created_by, now, now),
        )
        return dict(self.template_by_id(cursor.lastrowid))

    def quota(self, subject_type: str, subject_key: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_quotas WHERE subject_type=? AND subject_key=?", (subject_type, subject_key)).fetchone()

    def upsert_quota(self, *, subject_type: str, subject_key: str, max_queued: int, max_running: int, daily_submissions: int, actor: str, now: str) -> dict[str, Any]:
        self.connection.execute(
            "INSERT INTO compute_quotas(subject_type,subject_key,max_queued,max_running,daily_submissions,updated_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(subject_type,subject_key) DO UPDATE SET max_queued=excluded.max_queued,max_running=excluded.max_running,daily_submissions=excluded.daily_submissions,updated_by=excluded.updated_by,updated_at=excluded.updated_at",
            (subject_type, subject_key, max_queued, max_running, daily_submissions, actor, now, now),
        )
        return dict(self.quota(subject_type, subject_key))

    def count_user_states(self, requested_by: str) -> dict[str, int]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks WHERE requested_by=? GROUP BY status", (requested_by,)).fetchall()
        return {str(row["status"]): int(row["amount"]) for row in rows}

    def count_user_submissions_since(self, requested_by: str, since: str) -> int:
        return int(self.connection.execute("SELECT COUNT(*) FROM compute_tasks WHERE requested_by=? AND created_at>=?", (requested_by, since)).fetchone()[0])

    def task_by_id(self, task_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT t.*,tpl.code AS template_code,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id WHERE t.id=?", (task_id,)).fetchone()

    def task_by_idempotency(self, requested_by: str, key: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_tasks WHERE requested_by=? AND idempotency_key=?", (requested_by, key)).fetchone()

    def create_task(self, *, template_id: int, project_code: str, requested_by: str, parameters: dict[str, Any], parameter_digest: str, priority: int, idempotency_key: str, max_attempts: int, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO compute_tasks(template_id,project_code,requested_by,parameters_json,parameter_digest,priority,idempotency_key,status,attempt_count,max_attempts,available_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'queued',0,?,?,?,?)",
            (template_id, project_code, requested_by, json.dumps(parameters, ensure_ascii=False, sort_keys=True), parameter_digest, priority, idempotency_key, max_attempts, now, now, now),
        )
        return dict(self.task_by_id(cursor.lastrowid))

    def queued_candidate(self, capabilities: Iterable[str], now: str) -> sqlite3.Row | None:
        capability_list = sorted(set(capabilities))
        params: list[Any] = [now]
        condition = ""
        if capability_list:
            placeholders = ",".join("?" for _ in capability_list)
            condition = f" AND tpl.algorithm IN ({placeholders})"
            params.extend(capability_list)
        return self.connection.execute(
            "SELECT t.*,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id WHERE t.status='queued' AND t.available_at<=?" + condition + " ORDER BY t.priority DESC,t.created_at ASC,t.id ASC LIMIT 1",
            params,
        ).fetchone()

    def result_versions(self, task_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM compute_results WHERE task_id=? ORDER BY version", (task_id,)).fetchall()]

    def result_version(self, task_id: int, version: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_results WHERE task_id=? AND version=?", (task_id, version)).fetchone()

    def result_by_receipt(self, task_id: int, receipt: str) -> sqlite3.Row | None:
        if not receipt:
            return None
        return self.connection.execute("SELECT * FROM compute_results WHERE task_id=? AND completion_receipt=?", (task_id, receipt)).fetchone()

    def create_result(
        self,
        *,
        task_id: int,
        version: int,
        result: dict[str, Any],
        metrics: dict[str, Any],
        result_digest: str,
        completion_receipt: str,
        created_by: str,
        now: str,
    ) -> int:
        cursor = self.connection.execute(
            "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,completion_receipt,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (
                task_id,
                version,
                json.dumps(result, ensure_ascii=False, sort_keys=True),
                json.dumps(metrics, ensure_ascii=False, sort_keys=True),
                result_digest,
                completion_receipt,
                created_by,
                now,
            ),
        )
        return int(cursor.lastrowid)

    def add_artifact(
        self,
        *,
        task_id: int,
        result_version: int,
        relative_path: str,
        size_bytes: int,
        sha256: str,
        summary: str,
        purpose: str,
        role: str,
        retain_until: str,
        created_by: str,
        now: str,
    ) -> int:
        cursor = self.connection.execute(
            "INSERT INTO compute_result_artifacts(task_id,result_version,relative_path,size_bytes,sha256,summary,purpose,role,status,retain_until,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,'candidate',?,?,?)",
            (task_id, result_version, relative_path, size_bytes, sha256, summary, purpose, role, retain_until, created_by, now),
        )
        return int(cursor.lastrowid)

    def artifact_by_id(self, artifact_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_result_artifacts WHERE id=?", (artifact_id,)).fetchone()

    def artifacts(self, task_id: int, *, version: int | None = None) -> list[dict[str, Any]]:
        if version is None:
            rows = self.connection.execute("SELECT * FROM compute_result_artifacts WHERE task_id=? ORDER BY result_version,id", (task_id,)).fetchall()
        else:
            rows = self.connection.execute("SELECT * FROM compute_result_artifacts WHERE task_id=? AND result_version=? ORDER BY id", (task_id, version)).fetchall()
        return [dict(row) for row in rows]

    def all_artifacts(self, *, statuses: list[str] | None = None) -> list[dict[str, Any]]:
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            rows = self.connection.execute(
                f"SELECT * FROM compute_result_artifacts WHERE status IN ({placeholders}) ORDER BY id", statuses
            ).fetchall()
        else:
            rows = self.connection.execute("SELECT * FROM compute_result_artifacts ORDER BY id").fetchall()
        return [dict(row) for row in rows]

    def set_artifact_status(self, artifact_id: int, *, status: str, now: str, revoked_reason: str = "") -> None:
        if status == "published":
            self.connection.execute(
                "UPDATE compute_result_artifacts SET status='published',published_at=COALESCE(published_at,?),revoked_at=NULL,revoked_reason='',deleted_at=NULL WHERE id=?",
                (now, artifact_id),
            )
        elif status == "revoked":
            self.connection.execute(
                "UPDATE compute_result_artifacts SET status='revoked',revoked_at=?,revoked_reason=? WHERE id=?",
                (now, revoked_reason, artifact_id),
            )
        elif status == "deleted":
            self.connection.execute(
                "UPDATE compute_result_artifacts SET status='deleted',deleted_at=? WHERE id=?",
                (now, artifact_id),
            )
        else:
            raise ValueError(f"不支持的制品状态：{status}")

    def publish_result(self, task_id: int, version: int, *, actor: str, reason: str, now: str, retain_until: str) -> None:
        # 首次发布记录发布人/理由；重复发布只延长保留期，保留首次发布信息。
        self.connection.execute(
            "UPDATE compute_results SET published_at=COALESCE(published_at,?),published_by=CASE WHEN published_by='' THEN ? ELSE published_by END,publish_reason=CASE WHEN publish_reason='' THEN ? ELSE publish_reason END WHERE task_id=? AND version=?",
            (now, actor, reason, task_id, version),
        )
        self.connection.execute(
            "UPDATE compute_result_artifacts SET status='published',published_at=COALESCE(published_at,?),retain_until=MAX(retain_until,?),revoked_at=NULL,revoked_reason='',deleted_at=NULL WHERE task_id=? AND result_version=? AND role='permanent' AND status<>'revoked' AND status<>'deleted'",
            (now, retain_until, task_id, version),
        )

    def revoke_result(self, task_id: int, version: int, *, actor: str, reason: str, now: str) -> None:
        self.connection.execute(
            "UPDATE compute_results SET revoked_at=?,revoked_by=?,revoke_reason=? WHERE task_id=? AND version=?",
            (now, actor, reason, task_id, version),
        )
        self.connection.execute(
            "UPDATE compute_result_artifacts SET status='revoked',revoked_at=?,revoked_reason=? WHERE task_id=? AND result_version=? AND status='published'",
            (now, reason, task_id, version),
        )

    def revoke_artifacts(self, task_id: int, version: int, artifact_ids: list[int], *, reason: str, now: str) -> int:
        placeholders = ",".join("?" for _ in artifact_ids)
        cursor = self.connection.execute(
            f"UPDATE compute_result_artifacts SET status='revoked',revoked_at=?,revoked_reason=? WHERE task_id=? AND result_version=? AND id IN ({placeholders}) AND status IN ('published','candidate')",
            [now, reason, task_id, version, *artifact_ids],
        )
        return int(cursor.rowcount)

    def path_has_live_sibling(self, task_id: int, relative_path: str, *, exclude_artifact_id: int, now: str) -> bool:
        """同一路径是否还有已发布或仍在保留期内、且未删除的其他记录。"""
        row = self.connection.execute(
            "SELECT 1 FROM compute_result_artifacts "
            "WHERE task_id=? AND relative_path=? AND id<>? AND status<>'deleted' "
            "AND (status='published' OR (retain_until<>'' AND retain_until>?)) LIMIT 1",
            (task_id, relative_path, exclude_artifact_id, now),
        ).fetchone()
        return row is not None

    def path_referenced_by_published_result(self, task_id: int, relative_path: str, *, exclude_artifact_id: int | None = None) -> bool:
        sql = (
            "SELECT 1 FROM compute_result_artifacts a "
            "JOIN compute_results r ON r.task_id=a.task_id AND r.version=a.result_version "
            "WHERE a.task_id=? AND a.relative_path=? AND a.status='published' "
            "AND r.published_at IS NOT NULL AND r.revoked_at IS NULL"
        )
        params: list[Any] = [task_id, relative_path]
        if exclude_artifact_id is not None:
            sql += " AND a.id<>?"
            params.append(exclude_artifact_id)
        sql += " LIMIT 1"
        return self.connection.execute(sql, params).fetchone() is not None

    def interventions(self, task_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM compute_interventions WHERE task_id=? ORDER BY id", (task_id,)).fetchall()]

    def add_intervention(self, *, task_id: int, actor: str, action: str, reason: str, before: dict[str, Any], after: dict[str, Any], batch_key: str, now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_interventions(task_id,actor,action,reason,before_json,after_json,batch_key,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (task_id, actor, action, reason, json.dumps(before, ensure_ascii=False, sort_keys=True), json.dumps(after, ensure_ascii=False, sort_keys=True), batch_key, now),
        )

    def list_tasks(self, *, status: str | None, project_code: str | None, requested_by: str | None, limit: int) -> list[dict[str, Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        if status:
            clauses.append("t.status=?")
            values.append(status)
        if project_code:
            clauses.append("t.project_code=?")
            values.append(project_code)
        if requested_by:
            clauses.append("t.requested_by=?")
            values.append(requested_by)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        values.append(limit)
        rows = self.connection.execute(
            "SELECT t.*,tpl.code AS template_code,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id" + where + " ORDER BY t.priority DESC,t.created_at DESC,t.id DESC LIMIT ?",
            values,
        ).fetchall()
        return [dict(row) for row in rows]
