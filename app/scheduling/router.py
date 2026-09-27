from __future__ import annotations

from fastapi import APIRouter, Query

from app.scheduling.schemas import (
    ActivateRequest,
    CancelRequest,
    CheckpointRequest,
    ExtendRequest,
    ForceCheckpointRequest,
    ReleaseBatchRequest,
    RestoreItemRequest,
    SkipItemRequest,
    WindowCreate,
)
from app.scheduling.service import MaintenanceSchedulingService

router = APIRouter(prefix="/api/maintenance-scheduling", tags=["维护排程"])


def service() -> MaintenanceSchedulingService:
    return MaintenanceSchedulingService()


@router.post("/windows", status_code=201)
def create_window(payload: WindowCreate):
    return service().create_window(payload.model_dump())


@router.get("/windows")
def list_windows(status: str | None = None, limit: int = Query(default=100, ge=1, le=500)):
    return {"items": service().list_windows(status=status, limit=limit)}


@router.get("/windows/{code}")
def get_window(code: str):
    return service().get_window(code)


@router.get("/windows/{code}/preview")
def preview_window(code: str):
    return service().preview_window(code)


@router.post("/windows/{code}/activate")
def activate_window(code: str, payload: ActivateRequest):
    return service().activate(code, payload.actor)


@router.post("/windows/{code}/extend")
def extend_window(code: str, payload: ExtendRequest):
    return service().extend(code, payload.model_dump())


@router.post("/windows/{code}/cancel")
def cancel_window(code: str, payload: CancelRequest):
    return service().cancel(code, payload.model_dump())


@router.post("/windows/{code}/restore/begin")
def begin_restore(code: str, payload: ActivateRequest, force: bool = Query(default=False)):
    return service().begin_restore(code, payload.actor, force=force)


@router.post("/release-due")
def release_due(actor: str = Query(default="scheduler", min_length=1)):
    return service().run_due_releases(actor)


@router.get("/items/{item_id}")
def get_item(item_id: int):
    return service().get_item(item_id)


@router.post("/items/{item_id}/checkpoint")
def report_checkpoint(item_id: int, payload: CheckpointRequest):
    return service().report_checkpoint(item_id, payload.model_dump())


@router.post("/items/{item_id}/checkpoint/force")
def force_checkpoint(item_id: int, payload: ForceCheckpointRequest):
    return service().force_checkpoint(item_id, payload.model_dump())


@router.post("/items/{item_id}/checkpoint/failed")
def report_checkpoint_failure(item_id: int, payload: SkipItemRequest):
    return service().report_checkpoint_failure(item_id, payload.model_dump())


@router.post("/items/{item_id}/skip")
def skip_item(item_id: int, payload: SkipItemRequest):
    return service().skip_item(item_id, payload.model_dump())


@router.post("/items/{item_id}/restore")
def restore_item(item_id: int, payload: RestoreItemRequest):
    return service().restore_item(item_id, payload.model_dump())


@router.post("/items/{item_id}/restore/failed")
def report_restore_failure(item_id: int, payload: SkipItemRequest):
    return service().report_restore_failure(item_id, payload.model_dump())


@router.post("/batches/{batch_id}/release")
def release_batch(batch_id: int, payload: ReleaseBatchRequest):
    return service().release_batch(batch_id, payload.actor)


@router.get("/audit-events")
def list_audit(code: str | None = Query(default=None), limit: int = Query(default=100, ge=1, le=1000)):
    return service().list_audit(code=code, limit=limit)
