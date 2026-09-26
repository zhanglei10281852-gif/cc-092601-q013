from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.core.errors import ConflictError
from app.database import close_connection, get_connection, init_db

TEMPLATE = {
    "code": "solver-a",
    "name": "方程求解模板",
    "algorithm": "solver-a",
    "parameter_schema": {
        "iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000},
        "mode": {"type": "string", "required": True, "choices": ["fast", "accurate"]},
    },
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 3,
}


@pytest.fixture()
def artifact_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("TOWNSHIP_DATABASE_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("TOWNSHIP_ARTIFACT_ROOT", str(tmp_path / "artifacts"))
    close_connection()
    from app.main import app

    with TestClient(app) as test_client:
        yield test_client, tmp_path / "artifacts"
    close_connection()


def write_artifact(root: Path, relative: str, content: bytes, purpose: str) -> dict:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return {
        "relative_path": relative,
        "size_bytes": len(content),
        "digest": hashlib.sha256(content).hexdigest(),
        "purpose": purpose,
    }


def setup_template(client: TestClient) -> None:
    response = client.post("/api/compute/templates?actor=administrator", json=TEMPLATE)
    assert response.status_code == 201, response.text


def submit_and_claim(client: TestClient, key: str, user: str = "researcher-1", worker: str = "worker-1") -> dict:
    created = client.post(
        "/api/compute/tasks",
        json={
            "template_code": "solver-a",
            "project_code": "project-a",
            "requested_by": user,
            "parameters": {"iterations": 100, "mode": "accurate"},
            "priority": 50,
            "idempotency_key": key,
        },
    )
    assert created.status_code == 202, created.text
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": worker, "capabilities": ["solver-a"], "lease_seconds": 60})
    assert claimed.status_code == 200 and claimed.json()["task"], claimed.text
    return claimed.json()["task"]


def complete(client: TestClient, task_id: int, manifest: list[dict], receipt_key: str, worker: str = "worker-1", value: float = 1.5):
    return client.post(
        f"/api/compute/tasks/{task_id}/complete",
        json={"worker_id": worker, "result": {"value": value}, "metrics": {"seconds": 3}, "artifacts": manifest, "receipt_key": receipt_key},
    )


def test_complete_binds_verified_artifacts_to_result_version(artifact_client):
    client, root = artifact_client
    setup_template(client)
    task = submit_and_claim(client, "artifact-bind-0001")
    manifest = [
        write_artifact(root, "grid/final.vtk", b"grid-bytes", "grid"),
        write_artifact(root, "logs/run.log", b"log-lines", "log"),
        write_artifact(root, "checklist.json", b"{}", "checklist"),
    ]
    completed = complete(client, task["id"], manifest, "receipt-000001")
    assert completed.status_code == 200, completed.text
    assert completed.json()["status"] == "succeeded"
    assert completed.json()["replayed"] is False

    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    assert details["current_result_version"] == 1
    assert len(details["results"]) == 1
    version = details["results"][0]
    assert version["status"] == "candidate"
    assert version["receipt_key"] == "receipt-000001"
    assert version["retention_until"]
    assert [(item["relative_path"], item["purpose"]) for item in version["artifacts"]] == [
        ("checklist.json", "checklist"),
        ("grid/final.vtk", "grid"),
        ("logs/run.log", "log"),
    ]
    listed = client.get(f"/api/compute/tasks/{task['id']}/artifacts", params={"version": 1}).json()["items"]
    assert len(listed) == 3
    assert all(item["result_version"] == 1 for item in listed)


def test_artifact_verification_failure_keeps_task_running(artifact_client):
    client, root = artifact_client
    setup_template(client)
    task = submit_and_claim(client, "artifact-bad-0001")
    good = write_artifact(root, "grid.vtk", b"grid", "grid")

    missing = {"relative_path": "missing.log", "size_bytes": 3, "digest": hashlib.sha256(b"abc").hexdigest(), "purpose": "log"}
    assert complete(client, task["id"], [good, missing], "receipt-bad-001").status_code == 422

    tampered = dict(good, digest=hashlib.sha256(b"other-content").hexdigest())
    assert complete(client, task["id"], [tampered], "receipt-bad-002").status_code == 422

    wrong_size = dict(good, size_bytes=good["size_bytes"] + 1)
    assert complete(client, task["id"], [wrong_size], "receipt-bad-003").status_code == 422

    escape = dict(good, relative_path="../outside.txt")
    assert complete(client, task["id"], [escape], "receipt-bad-004").status_code == 422

    duplicated = [good, dict(good)]
    assert complete(client, task["id"], duplicated, "receipt-bad-005").status_code == 422

    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    assert details["status"] == "running"
    assert details["results"] == []
    assert client.get(f"/api/compute/tasks/{task['id']}/artifacts").json()["items"] == []

    recovered = complete(client, task["id"], [good], "receipt-bad-006")
    assert recovered.status_code == 200 and recovered.json()["status"] == "succeeded"


def test_duplicate_receipt_does_not_create_second_artifact_records(artifact_client):
    client, root = artifact_client
    setup_template(client)
    task = submit_and_claim(client, "artifact-dup-0001")
    manifest = [write_artifact(root, "grid.vtk", b"grid", "grid")]

    first = complete(client, task["id"], manifest, "receipt-dup-001")
    assert first.status_code == 200
    second = complete(client, task["id"], manifest, "receipt-dup-001")
    assert second.status_code == 200 and second.json()["replayed"] is True

    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    assert len(details["results"]) == 1
    assert len(details["results"][0]["artifacts"]) == 1

    changed = client.post(
        f"/api/compute/tasks/{task['id']}/complete",
        json={"worker_id": "worker-1", "result": {"value": 999}, "metrics": {"seconds": 3}, "artifacts": manifest, "receipt_key": "receipt-dup-001"},
    )
    assert changed.status_code == 409


def test_download_authorization_checks_permission_and_version_state(artifact_client):
    client, root = artifact_client
    setup_template(client)
    task = submit_and_claim(client, "artifact-dl-0001", user="researcher-1")
    manifest = [write_artifact(root, "grid.vtk", b"grid", "grid")]
    assert complete(client, task["id"], manifest, "receipt-dl-0001").status_code == 200

    granted = client.post("/api/compute/downloads/authorize", json={"task_id": task["id"], "requester": "researcher-1"})
    assert granted.status_code == 200, granted.text
    body = granted.json()
    assert body["version"] == 1 and body["status"] == "candidate"
    assert body["grant_expires_at"]
    assert [item["relative_path"] for item in body["artifacts"]] == ["grid.vtk"]

    denied = client.post("/api/compute/downloads/authorize", json={"task_id": task["id"], "requester": "intruder"})
    assert denied.status_code == 403

    bootstrap = client.post("/api/auth/bootstrap", json={"username": "admin", "password": "Admin!23456", "client_label": "tests"})
    assert bootstrap.status_code == 201, bootstrap.text
    admin_grant = client.post("/api/compute/downloads/authorize", json={"task_id": task["id"], "requester": "admin"})
    assert admin_grant.status_code == 200

    withdrawn = client.post(
        f"/api/compute/tasks/{task['id']}/results/1/withdraw",
        json={"actor": "administrator", "reason": "数据存在污染"},
    )
    assert withdrawn.status_code == 200 and withdrawn.json()["status"] == "withdrawn"
    blocked = client.post("/api/compute/downloads/authorize", json={"task_id": task["id"], "requester": "researcher-1"})
    assert blocked.status_code == 409


def test_retention_expiry_blocks_download_authorization(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("TOWNSHIP_DATABASE_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("TOWNSHIP_ARTIFACT_ROOT", str(tmp_path / "artifacts"))
    close_connection()
    init_db()
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(TEMPLATE, "administrator")
    task = service.submit(
        {
            "template_code": "solver-a",
            "project_code": "project-a",
            "requested_by": "researcher-1",
            "parameters": {"iterations": 100, "mode": "accurate"},
            "priority": 50,
            "idempotency_key": "retention-000001",
        }
    )
    claimed = service.claim("worker-1", ["solver-a"], 60)
    assert claimed and claimed["id"] == task["id"]
    manifest = [write_artifact(tmp_path / "artifacts", "grid.vtk", b"grid", "grid")]
    completed = service.complete(task["id"], "worker-1", {"value": 1}, {}, artifacts=manifest, receipt_key="receipt-rt-001", retention_days=1)
    assert completed["status"] == "succeeded"

    granted = service.authorize_download(task["id"], None, "researcher-1")
    assert granted["version"] == 1 and granted["retention_until"]

    clock.advance(days=2)
    with pytest.raises(ConflictError):
        service.authorize_download(task["id"], None, "researcher-1")
    close_connection()


def test_publish_withdraw_lifecycle_rules(artifact_client):
    client, root = artifact_client
    setup_template(client)
    task = submit_and_claim(client, "lifecycle-0001")
    manifest = [write_artifact(root, "grid.vtk", b"grid", "grid")]
    assert complete(client, task["id"], manifest, "receipt-lc-001").status_code == 200

    published = client.post(f"/api/compute/tasks/{task['id']}/results/1/publish?actor=administrator")
    assert published.status_code == 200 and published.json()["status"] == "published"
    assert published.json()["artifacts"][0]["relative_path"] == "grid.vtk"
    again = client.post(f"/api/compute/tasks/{task['id']}/results/1/publish?actor=administrator")
    assert again.status_code == 200

    withdrawn = client.post(
        f"/api/compute/tasks/{task['id']}/results/1/withdraw",
        json={"actor": "administrator", "reason": "复核未通过"},
    )
    assert withdrawn.status_code == 200 and withdrawn.json()["status"] == "withdrawn"
    republish = client.post(f"/api/compute/tasks/{task['id']}/results/1/publish?actor=administrator")
    assert republish.status_code == 409
    missing = client.post(f"/api/compute/tasks/{task['id']}/results/9/publish?actor=administrator")
    assert missing.status_code == 404

    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    assert [item["action"] for item in details["interventions"]] == ["publish_result", "withdraw_result"]


def test_cleanup_plan_classifies_and_protects_referenced_artifacts(artifact_client):
    client, root = artifact_client
    setup_template(client)

    task_a = submit_and_claim(client, "cleanup-a-0001")
    assert complete(client, task_a["id"], [write_artifact(root, "a/grid.vtk", b"a-grid", "grid")], "receipt-cl-a01").status_code == 200

    task_b = submit_and_claim(client, "cleanup-b-0001")
    assert complete(client, task_b["id"], [write_artifact(root, "b/grid.vtk", b"b-grid", "grid")], "receipt-cl-b01").status_code == 200
    published = client.post(f"/api/compute/tasks/{task_b['id']}/results/1/publish?actor=administrator")
    assert published.status_code == 200

    task_c = submit_and_claim(client, "cleanup-c-0001")
    assert complete(client, task_c["id"], [write_artifact(root, "c/grid.vtk", b"c-grid", "grid")], "receipt-cl-c01").status_code == 200
    assert client.post(
        f"/api/compute/tasks/{task_c['id']}/results/1/withdraw",
        json={"actor": "administrator", "reason": "结果废弃"},
    ).status_code == 200
    get_connection().execute("UPDATE compute_results SET retention_until='2000-01-01T00:00:00+00:00' WHERE task_id=?", (task_c["id"],))

    task_d = submit_and_claim(client, "cleanup-d-0001")
    assert complete(client, task_d["id"], [write_artifact(root, "d/old-grid.vtk", b"old-grid", "grid")], "receipt-cl-d01").status_code == 200
    retried = client.post(f"/api/compute/tasks/{task_d['id']}/retry", json={"actor": "administrator", "reason": "重新计算修正网格"})
    assert retried.status_code == 200 and retried.json()["status"] == "queued"
    reclaimed = client.post("/api/compute/tasks/claim", json={"worker_id": "worker-2", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert reclaimed.json()["task"]["id"] == task_d["id"]
    assert complete(client, task_d["id"], [write_artifact(root, "d/new-grid.vtk", b"new-grid", "grid")], "receipt-cl-d02", worker="worker-2", value=2.5).status_code == 200

    details = client.get(f"/api/compute/task-details/{task_d['id']}").json()
    assert details["current_result_version"] == 2
    assert [item["version"] for item in details["results"]] == [1, 2]

    plan = client.get("/api/compute/cleanup/plan").json()
    classes = plan["classes"]

    published_items = classes["published"]
    assert [item["task_id"] for item in published_items] == [task_b["id"]]
    assert published_items[0]["deletable"] is False
    assert "引用" in published_items[0]["block_reason"]

    candidate_items = classes["candidate"]
    assert {(item["task_id"], item["result_version"]) for item in candidate_items} == {(task_a["id"], 1), (task_d["id"], 2)}
    assert all(item["deletable"] is False for item in candidate_items)
    assert all(item["block_reason"] for item in candidate_items)

    withdrawn_items = classes["withdrawn"]
    assert [item["task_id"] for item in withdrawn_items] == [task_c["id"]]
    assert withdrawn_items[0]["deletable"] is True

    temporary_items = classes["temporary"]
    assert [(item["task_id"], item["result_version"]) for item in temporary_items] == [(task_d["id"], 1)]
    assert temporary_items[0]["deletable"] is True

    assert plan["summary"]["published"]["deletable"] == 0
    assert plan["summary"]["temporary"]["deletable"] == 1
    assert plan["summary"]["withdrawn"]["deletable"] == 1
