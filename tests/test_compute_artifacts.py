from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime
from pathlib import Path

from app.compute.artifacts import ArtifactStore
from app.compute.service import TEMPORARY_ARTIFACT_RETENTION_DAYS, ComputeOperationsService
from app.core.clock import FrozenClock
from app.core.config import Settings
from app.database import get_connection

TEMPLATE = {
    "code": "solver-a",
    "name": "方程求解模板",
    "algorithm": "solver-a",
    "parameter_schema": {
        "iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000},
        "tolerance": {"type": "number", "required": False, "minimum": 0.0, "maximum": 1.0},
        "mode": {"type": "string", "required": True, "choices": ["fast", "accurate"]},
    },
    "default_parameters": {"tolerance": 0.001},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
}


def submit_payload(key: str, *, user: str = "researcher-1", priority: int = 50) -> dict:
    return {
        "template_code": "solver-a",
        "project_code": "project-a",
        "requested_by": user,
        "parameters": {"iterations": 100, "mode": "accurate"},
        "priority": priority,
        "idempotency_key": key,
    }


def create_template(client) -> None:
    response = client.post("/api/compute/templates?actor=administrator", json=TEMPLATE)
    assert response.status_code == 201, response.text


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def artifact_root() -> Path:
    return Path(os.environ["TOWNSHIP_ARTIFACT_ROOT"])


def write_file(relative: str, data: bytes, task_id: int) -> dict:
    path = artifact_root() / f"task-{task_id}" / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return {"relative_path": relative, "size_bytes": len(data), "sha256": sha256_bytes(data)}


def mesh_entry(relative: str = "run-1/grid.mesh", data: bytes = b"mesh-binary-v1", task_id: int | None = None) -> dict:
    entry = write_file(relative, data, task_id if task_id is not None else 0)
    entry.update({"summary": "结果网格文件", "purpose": "mesh", "role": "permanent"})
    return entry


def log_entry(relative: str = "run-1/solver.log", data: bytes = b"converged in 42 iterations\n", task_id: int | None = None) -> dict:
    entry = write_file(relative, data, task_id if task_id is not None else 0)
    entry.update({"summary": "求解日志摘要", "purpose": "log", "role": "temporary"})
    return entry


def claim_and_complete(client, task_id: int, *, worker: str = "w1", receipt: str = "", artifacts: list[dict] | None = None, claim: bool = True):
    if claim:
        claimed = client.post("/api/compute/tasks/claim", json={"worker_id": worker, "capabilities": ["solver-a"], "lease_seconds": 300})
        assert claimed.status_code == 200 and claimed.json()["task"]["id"] == task_id
    payload = {"worker_id": worker, "result": {"value": 3.14}, "metrics": {"seconds": 2}, "artifacts": artifacts or []}
    if receipt:
        payload["completion_receipt"] = receipt
    return client.post(f"/api/compute/tasks/{task_id}/complete", json=payload)


def prepare_task(client, key: str = "artifact-000001", *, user: str = "researcher-1") -> int:
    response = client.post("/api/compute/tasks", json=submit_payload(key, user=user))
    assert response.status_code == 202, response.text
    return response.json()["id"]


def test_complete_binds_verified_artifacts_to_result_version(client):
    create_template(client)
    task_id = prepare_task(client)
    completed = claim_and_complete(client, task_id, artifacts=[mesh_entry(task_id=task_id), log_entry(task_id=task_id)])
    assert completed.status_code == 200, completed.text
    assert completed.json()["status"] == "succeeded"
    details = client.get(f"/api/compute/task-details/{task_id}").json()
    assert details["current_result_version"] == 1
    listing = client.get(f"/api/compute/tasks/{task_id}/artifacts").json()["items"]
    assert {item["relative_path"] for item in listing} == {"run-1/grid.mesh", "run-1/solver.log"}
    mesh = next(item for item in listing if item["purpose"] == "mesh")
    assert mesh["status"] == "candidate" and mesh["result_version"] == 1 and mesh["size_bytes"] == len(b"mesh-binary-v1")


def test_size_mismatch_fails_verification_and_keeps_task_running(client):
    create_template(client)
    task_id = prepare_task(client, "artifact-bad-size")
    entry = mesh_entry(task_id=task_id)
    entry["size_bytes"] += 10
    response = claim_and_complete(client, task_id, artifacts=[entry])
    assert response.status_code == 422
    details = client.get(f"/api/compute/task-details/{task_id}").json()
    assert details["status"] == "running"
    assert details["results"] == []
    assert client.get(f"/api/compute/tasks/{task_id}/artifacts").json()["items"] == []


def test_sha256_mismatch_and_missing_file_fail(client):
    create_template(client)
    bad_digest = prepare_task(client, "artifact-bad-digest")
    entry = mesh_entry("run-bad/grid.mesh", task_id=bad_digest)
    entry["sha256"] = "0" * 64
    response = claim_and_complete(client, bad_digest, artifacts=[entry])
    assert response.status_code == 422
    missing = prepare_task(client, "artifact-missing")
    response = claim_and_complete(client, missing, artifacts=[{"relative_path": "nope/x.bin", "size_bytes": 1, "sha256": "0" * 64, "summary": "缺失文件", "purpose": "other", "role": "permanent"}])
    assert response.status_code == 422
    for task_id in (bad_digest, missing):
        details = client.get(f"/api/compute/task-details/{task_id}").json()
        assert details["status"] == "running" and details["results"] == []


def unsafe_entry(relative: str) -> dict:
    return {"relative_path": relative, "size_bytes": 1, "sha256": "0" * 64, "summary": "越界路径", "purpose": "other", "role": "permanent"}


def test_unsafe_and_duplicate_manifest_paths_rejected(client):
    create_template(client)
    task_id = prepare_task(client, "artifact-unsafe-paths")
    response = claim_and_complete(client, task_id, artifacts=[unsafe_entry("../escape.mesh")])
    assert response.status_code == 422
    response = claim_and_complete(client, task_id, artifacts=[unsafe_entry("/tmp/escape.mesh")], claim=False)
    assert response.status_code == 422
    entry = mesh_entry(task_id=task_id)
    response = claim_and_complete(client, task_id, artifacts=[entry, dict(entry)], claim=False)
    assert response.status_code == 422
    details = client.get(f"/api/compute/task-details/{task_id}").json()
    assert details["status"] == "running"


def test_duplicate_completion_receipt_is_idempotent(client):
    create_template(client)
    task_id = prepare_task(client, "artifact-receipt")
    first = claim_and_complete(client, task_id, receipt="receipt-aaa-1", artifacts=[mesh_entry(task_id=task_id)])
    assert first.status_code == 200
    # 租约已释放，模拟同一工作者重发回执（网络重试）。
    second = client.post(
        f"/api/compute/tasks/{task_id}/complete",
        json={"worker_id": "w1", "result": {"value": 9.99}, "metrics": {}, "completion_receipt": "receipt-aaa-1", "artifacts": []},
    )
    assert second.status_code == 200
    details = client.get(f"/api/compute/task-details/{task_id}").json()
    assert len(details["results"]) == 1
    assert details["results"][0]["completion_receipt"] == "receipt-aaa-1"
    assert len(client.get(f"/api/compute/tasks/{task_id}/artifacts").json()["items"]) == 1
    # 其他工作者冒用同一回执必须被拒绝。
    impostor = client.post(
        f"/api/compute/tasks/{task_id}/complete",
        json={"worker_id": "w2", "result": {}, "metrics": {}, "completion_receipt": "receipt-aaa-1", "artifacts": []},
    )
    assert impostor.status_code == 409


def _publish(client, task_id: int, version: int = 1, actor: str = "operator-1"):
    return client.post(
        f"/api/compute/tasks/{task_id}/results/{version}/publish",
        json={"actor": actor, "reason": "验收通过，对外发布"},
    )


def test_publish_download_authorize_and_fetch_file(client):
    create_template(client)
    task_id = prepare_task(client, "artifact-download", user="researcher-1")
    mesh = mesh_entry(task_id=task_id)
    completed = claim_and_complete(client, task_id, artifacts=[mesh, log_entry(task_id=task_id)])
    assert completed.status_code == 200
    mesh_id = next(item["id"] for item in client.get(f"/api/compute/tasks/{task_id}/artifacts").json()["items"] if item["purpose"] == "mesh")
    # 未发布：任何人不能授权下载。
    denied = client.post(f"/api/compute/artifacts/{mesh_id}/download-authorization", json={"requester": "researcher-1"})
    assert denied.status_code == 409
    published = _publish(client, task_id)
    assert published.status_code == 200, published.text
    listing = {item["relative_path"]: item for item in client.get(f"/api/compute/tasks/{task_id}/artifacts").json()["items"]}
    assert listing["run-1/grid.mesh"]["status"] == "published"
    # 临时制品不会随版本发布，也无法下载。
    assert listing["run-1/solver.log"]["status"] == "candidate"
    # 任务提交者可授权；无关用户 403；管理员可授权。
    owner = client.post(f"/api/compute/artifacts/{mesh_id}/download-authorization", json={"requester": "researcher-1"})
    assert owner.status_code == 200
    stranger = client.post(f"/api/compute/artifacts/{mesh_id}/download-authorization", json={"requester": "someone-else"})
    assert stranger.status_code == 403
    bootstrap = client.post("/api/auth/bootstrap", json={"username": "admin", "password": "Admin!23456", "client_label": "tests"})
    assert bootstrap.status_code == 201
    admin = client.post(f"/api/compute/artifacts/{mesh_id}/download-authorization", json={"requester": "admin"})
    assert admin.status_code == 200
    authorization = owner.json()
    assert authorization["result_version"] == 1
    fetched = client.get(authorization["download_url"])
    assert fetched.status_code == 200
    assert fetched.content == b"mesh-binary-v1"
    assert fetched.headers["x-artifact-sha256"] == mesh["sha256"]
    # 篡改令牌无效。
    tampered = authorization["download_url"][:-2] + ("aa" if authorization["download_url"][-2:] != "aa" else "bb")
    assert client.get(tampered).status_code == 403


def test_revoked_version_cannot_be_downloaded(client):
    create_template(client)
    task_id = prepare_task(client, "artifact-revoke")
    claim_and_complete(client, task_id, artifacts=[mesh_entry(task_id=task_id)])
    mesh_id = next(item["id"] for item in client.get(f"/api/compute/tasks/{task_id}/artifacts").json()["items"] if item["purpose"] == "mesh")
    _publish(client, task_id)
    revoked = client.post(
        f"/api/compute/tasks/{task_id}/results/1/revoke",
        json={"actor": "operator-1", "reason": "发现数据瑕疵，撤回结果"},
    )
    assert revoked.status_code == 200
    listing = {item["id"]: item for item in client.get(f"/api/compute/tasks/{task_id}/artifacts").json()["items"]}
    assert listing[mesh_id]["status"] == "revoked"
    assert client.post(f"/api/compute/artifacts/{mesh_id}/download-authorization", json={"requester": "researcher-1"}).status_code == 409
    details = client.get(f"/api/compute/task-details/{task_id}").json()
    assert details["results"][0]["revoke_reason"] == "发现数据瑕疵，撤回结果"


def _frozen_service(root: Path, *, retention_days: int) -> tuple[ComputeOperationsService, FrozenClock]:
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    settings = Settings(
        database_path=Path(os.environ["TOWNSHIP_DATABASE_PATH"]),
        session_ttl_minutes=480,
        login_failure_limit=5,
        login_lock_minutes=30,
        default_page_size=20,
        audit_retention_days=365,
        job_lease_seconds=60,
        artifact_storage_root=root,
        artifact_retention_days=retention_days,
        artifact_download_secret="test-download-secret",
    )
    store = ArtifactStore(root)
    return ComputeOperationsService(get_connection(), clock, artifact_store=store, settings=settings), clock


def test_cleanup_plan_classifies_and_blocks_referenced(client):
    create_template(client)
    service, clock = _frozen_service(artifact_root(), retention_days=0)
    task_published = service.submit(submit_payload("cleanup-published"))
    assert service.claim("w1", ["solver-a"], 300)["id"] == task_published["id"]
    service.complete(
        task_published["id"], "w1", {"value": 1}, {},
        completion_receipt="rcpt-published",
        artifacts=[mesh_entry("pub/grid.mesh", task_id=task_published["id"]), log_entry("pub/solver.log", task_id=task_published["id"])],
    )
    service.publish_result_version(task_published["id"], 1, "operator-1", "发布")
    task_candidate = service.submit(submit_payload("cleanup-candidate"))
    assert service.claim("w2", ["solver-a"], 300)["id"] == task_candidate["id"]
    service.complete(
        task_candidate["id"], "w2", {"value": 2}, {},
        completion_receipt="rcpt-candidate",
        artifacts=[mesh_entry("cand/grid.mesh", b"candidate-bytes", task_id=task_candidate["id"])],
    )
    task_revoked = service.submit(submit_payload("cleanup-revoked"))
    assert service.claim("w3", ["solver-a"], 300)["id"] == task_revoked["id"]
    service.complete(
        task_revoked["id"], "w3", {"value": 3}, {},
        completion_receipt="rcpt-revoked",
        artifacts=[mesh_entry("rev/grid.mesh", b"revoked-bytes", task_id=task_revoked["id"])],
    )
    service.publish_result_version(task_revoked["id"], 1, "operator-1", "发布")
    service.revoke_result_version(task_revoked["id"], 1, "operator-1", "撤回")

    plan = service.cleanup_plan()
    by_path = {item["relative_path"]: item for group in ("temporary", "candidate", "published", "revoked") for item in plan[group]}
    assert by_path["pub/grid.mesh"]["status"] == "published" and by_path["pub/grid.mesh"]["eligible"] is False
    assert by_path["pub/solver.log"]["role"] == "temporary" and by_path["pub/solver.log"]["eligible"] is False
    assert by_path["cand/grid.mesh"]["status"] == "candidate" and by_path["cand/grid.mesh"]["eligible"] is True
    assert by_path["rev/grid.mesh"]["status"] == "revoked" and by_path["rev/grid.mesh"]["eligible"] is True
    blocked_ids = {item["id"] for item in plan["blocked"]}
    assert by_path["pub/grid.mesh"]["id"] in blocked_ids
    assert by_path["cand/grid.mesh"]["id"] not in blocked_ids

    result = service.execute_cleanup()
    removed_paths = {item["relative_path"] for item in result["deleted"]}
    assert "cand/grid.mesh" in removed_paths and "rev/grid.mesh" in removed_paths
    assert "pub/grid.mesh" not in removed_paths
    assert not (artifact_root() / f"task-{task_candidate['id']}/cand/grid.mesh").exists()
    assert not (artifact_root() / f"task-{task_revoked['id']}/rev/grid.mesh").exists()
    assert (artifact_root() / f"task-{task_published['id']}/pub/grid.mesh").exists()

    # 临时制品超过短期保留期后进入可清理集合，已发布网格始终受保护。
    clock.advance(days=TEMPORARY_ARTIFACT_RETENTION_DAYS + 1)
    plan = service.cleanup_plan()
    log_item = next(item for item in plan["temporary"] if item["relative_path"] == "pub/solver.log")
    assert log_item["eligible"] is True
    result = service.execute_cleanup()
    assert "pub/solver.log" in {item["relative_path"] for item in result["deleted"]}
    assert (artifact_root() / f"task-{task_published['id']}/pub/grid.mesh").exists()


def _reopen_succeeded_task(task_id: int, worker: str = "w1") -> None:
    """模拟重算调度器把已成功任务重新置为运行中，使 complete 能产生下一结果版本。"""
    from app.database import transaction

    with transaction(immediate=True) as connection:
        connection.execute(
            "UPDATE compute_tasks SET status='running',lease_owner=?,finished_at=NULL,current_result_version=NULL WHERE id=?",
            (worker, task_id),
        )


def test_candidate_reusing_published_path_is_protected_from_cleanup(client):
    create_template(client)
    service, clock = _frozen_service(artifact_root(), retention_days=0)
    task = service.submit(submit_payload("cleanup-shared-path"))
    assert service.claim("w1", ["solver-a"], 300)["id"] == task["id"]
    shared = mesh_entry("shared/grid.mesh", b"shared-bytes", task_id=task["id"])
    service.complete(task["id"], "w1", {"v": 1}, {}, completion_receipt="rcpt-v1", artifacts=[shared])
    service.publish_result_version(task["id"], 1, "operator-1", "发布 v1")

    # 重算产出 v2，复用同一相对路径且内容一致（物理文件不被覆盖）。
    _reopen_succeeded_task(task["id"])
    service.complete(task["id"], "w1", {"v": 2}, {}, completion_receipt="rcpt-v2", artifacts=[dict(shared)])
    plan = service.cleanup_plan()
    v2_candidate = next(item for item in plan["candidate"] if item["result_version"] == 2)
    assert v2_candidate["eligible"] is False
    assert "referenced_by_published_result" in v2_candidate["reasons"]
    service.execute_cleanup()
    assert (artifact_root() / f"task-{task['id']}/shared/grid.mesh").exists()


def test_reusing_same_path_with_different_content_rejected(client):
    create_template(client)
    service, _ = _frozen_service(artifact_root(), retention_days=90)
    task = service.submit(submit_payload("shared-path-content-change"))
    service.claim("w1", ["solver-a"], 300)
    service.complete(
        task["id"], "w1", {"v": 1}, {}, completion_receipt="rcpt-a",
        artifacts=[mesh_entry("shared/x.mesh", b"original", task_id=task["id"])],
    )
    _reopen_succeeded_task(task["id"])
    from app.core.errors import ConflictError

    try:
        service.complete(
            task["id"], "w1", {"v": 2}, {}, completion_receipt="rcpt-b",
            artifacts=[mesh_entry("shared/x.mesh", b"changed-content", task_id=task["id"])],
        )
    except ConflictError:
        pass
    else:
        raise AssertionError("同一路径提交不同内容必须被拒绝")
    details = service.get_task(task["id"])
    assert details["status"] == "running" and len(details["results"]) == 1


def test_cleanup_plan_api_end_to_end(client):
    create_template(client)
    task_id = prepare_task(client, "cleanup-api")
    claim_and_complete(client, task_id, artifacts=[mesh_entry(task_id=task_id), log_entry(task_id=task_id)])
    listing = client.get(f"/api/compute/tasks/{task_id}/artifacts").json()["items"]
    mesh_id = next(item["id"] for item in listing if item["purpose"] == "mesh")

    plan = client.get("/api/compute/artifacts/cleanup/plan")
    assert plan.status_code == 200
    body = plan.json()
    assert {"temporary", "candidate", "published", "revoked", "blocked", "eligible_artifact_ids"} <= set(body)
    assert {item["relative_path"] for item in body["candidate"]} == {"run-1/grid.mesh"}
    assert {item["relative_path"] for item in body["temporary"]} == {"run-1/solver.log"}
    assert body["eligible_artifact_ids"] == []

    executed = client.post("/api/compute/artifacts/cleanup/execute")
    assert executed.status_code == 200 and executed.json()["deleted"] == []

    client.post(f"/api/compute/tasks/{task_id}/results/1/publish", json={"actor": "operator-1", "reason": "发布"})
    client.post(f"/api/compute/tasks/{task_id}/results/1/revoke", json={"actor": "operator-1", "reason": "撤回"})
    plan = client.get("/api/compute/artifacts/cleanup/plan").json()
    revoked_paths = {item["relative_path"] for item in plan["revoked"]}
    assert "run-1/grid.mesh" in revoked_paths
    # 撤回仍在保留期内，且文件未被删除。
    assert all(item["eligible"] is False for item in plan["revoked"])
    assert (artifact_root() / f"task-{task_id}/run-1/grid.mesh").exists()
    blocked_ids = {item["id"] for item in plan["blocked"]}
    assert mesh_id in blocked_ids


def test_download_denied_after_retention_expires(client):
    create_template(client)
    service, clock = _frozen_service(artifact_root(), retention_days=1)
    task_id = service.submit(submit_payload("download-retention"))
    service.claim("w1", ["solver-a"], 300)
    service.complete(task_id["id"], "w1", {"v": 1}, {}, completion_receipt="rcpt-retention", artifacts=[mesh_entry("ret/grid.mesh", task_id=task_id["id"])])
    service.publish_result_version(task_id["id"], 1, "operator-1", "发布")
    artifacts = service.list_artifacts(task_id["id"])
    mesh_id = next(item["id"] for item in artifacts if item["purpose"] == "mesh")
    authorization = service.authorize_download(mesh_id, "researcher-1")
    assert "/download" in authorization["download_url"]
    clock.advance(days=1, minutes=1)
    from app.core.errors import ConflictError

    try:
        service.authorize_download(mesh_id, "researcher-1")
    except ConflictError:
        pass
    else:
        raise AssertionError("超过保留期后不应再授权下载")
