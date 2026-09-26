from __future__ import annotations

from fastapi import APIRouter, Query
from fastapi.responses import FileResponse

from app.compute.schemas import (
    BatchOperation,
    CancelRequest,
    DownloadAuthorizationRequest,
    PriorityRequest,
    PublishRequest,
    QuotaSet,
    RetryRequest,
    RevokeRequest,
    TaskClaim,
    TaskFailure,
    TaskResult,
    TaskSubmit,
    TemplateCreate,
)
from app.compute.service import ComputeOperationsService

router = APIRouter(prefix="/api/compute", tags=["科学计算任务运营"])


def service() -> ComputeOperationsService:
    return ComputeOperationsService()


@router.get("/templates")
def list_templates():
    return {"items": service().list_templates()}


@router.post("/templates", status_code=201)
def create_template(payload: TemplateCreate, actor: str = Query(..., min_length=1)):
    return service().create_template(payload.model_dump(), actor)


@router.put("/quotas")
def set_quota(payload: QuotaSet, actor: str = Query(..., min_length=1)):
    return service().set_quota(payload.model_dump(), actor)


@router.post("/tasks", status_code=202)
def submit_task(payload: TaskSubmit):
    return service().submit(payload.model_dump())


@router.get("/tasks")
def list_tasks(status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = Query(default=100, ge=1, le=500)):
    return {"items": service().list_tasks(status=status, project_code=project_code, requested_by=requested_by, limit=limit)}


@router.get("/task-details/{task_id}")
def get_task(task_id: int):
    return service().get_task(task_id)


@router.post("/tasks/claim")
def claim_task(payload: TaskClaim):
    return {"task": service().claim(payload.worker_id, payload.capabilities, payload.lease_seconds)}


@router.post("/tasks/{task_id}/heartbeat")
def heartbeat(task_id: int, payload: TaskClaim):
    return service().heartbeat(task_id, payload.worker_id, payload.lease_seconds)


@router.post("/tasks/{task_id}/complete")
def complete_task(task_id: int, payload: TaskResult):
    data = payload.model_dump()
    return service().complete(
        task_id,
        data["worker_id"],
        data["result"],
        data["metrics"],
        completion_receipt=data["completion_receipt"],
        artifacts=data["artifacts"],
    )


@router.post("/tasks/{task_id}/fail")
def fail_task(task_id: int, payload: TaskFailure):
    return service().fail(task_id, payload.worker_id, payload.error_code, payload.message, payload.retryable)


@router.post("/tasks/{task_id}/cancel")
def cancel_task(task_id: int, payload: CancelRequest):
    return service().cancel(task_id, payload.actor, payload.reason)


@router.post("/tasks/{task_id}/retry")
def retry_task(task_id: int, payload: RetryRequest):
    return service().retry(task_id, payload.actor, payload.reason, payload.priority)


@router.post("/tasks/{task_id}/priority")
def set_priority(task_id: int, payload: PriorityRequest):
    return service().set_priority(task_id, payload.actor, payload.reason, payload.priority)


@router.post("/tasks/batch")
def batch_operation(payload: BatchOperation):
    return service().batch_operation(payload.model_dump())


@router.post("/recovery/expired-leases")
def recover_expired(actor: str = Query(default="recovery-worker", min_length=1)):
    return service().recover_expired(actor)


@router.get("/tasks/{task_id}/artifacts")
def list_artifacts(task_id: int, version: int | None = Query(default=None, ge=1)):
    return {"items": service().list_artifacts(task_id, version=version)}


@router.post("/tasks/{task_id}/results/{version}/publish")
def publish_result(task_id: int, version: int, payload: PublishRequest):
    return service().publish_result_version(task_id, version, payload.actor, payload.reason)


@router.post("/tasks/{task_id}/results/{version}/revoke")
def revoke_result(task_id: int, version: int, payload: RevokeRequest):
    return service().revoke_result_version(task_id, version, payload.actor, payload.reason, payload.artifact_ids)


@router.post("/artifacts/{artifact_id}/download-authorization")
def authorize_artifact_download(artifact_id: int, payload: DownloadAuthorizationRequest):
    return service().authorize_download(artifact_id, payload.requester)


@router.get("/artifacts/{artifact_id}/download")
def download_artifact(artifact_id: int, token: str = Query(..., min_length=10)):
    artifact, path = service().resolve_download(artifact_id, token)
    return FileResponse(
        path,
        filename=artifact["relative_path"].rsplit("/", 1)[-1],
        headers={
            "X-Artifact-Id": str(artifact["id"]),
            "X-Artifact-Sha256": artifact["sha256"],
            "X-Artifact-Version": str(artifact["result_version"]),
        },
    )


@router.get("/artifacts/cleanup/plan")
def cleanup_plan():
    return service().cleanup_plan()


@router.post("/artifacts/cleanup/execute")
def execute_cleanup(actor: str = Query(default="cleanup-worker", min_length=1)):
    return service().execute_cleanup(actor)


@router.get("/summary")
def summary():
    return service().summary()
