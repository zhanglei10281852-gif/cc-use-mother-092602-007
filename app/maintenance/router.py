from __future__ import annotations

from fastapi import APIRouter, Query

from app.maintenance.schemas import (
    ActivateRequest,
    BatchRelease,
    CancelWindowRequest,
    CheckpointSubmit,
    EndDrainRequest,
    ExceptionReport,
    ExceptionResolve,
    ExtendRequest,
    ItemRestore,
    ItemSkip,
    WindowCreate,
)
from app.maintenance.service import MaintenanceScheduleService

router = APIRouter(prefix="/api/maintenance-windows", tags=["维护排程"])


def service() -> MaintenanceScheduleService:
    return MaintenanceScheduleService()


@router.post("", status_code=201)
def create_window(payload: WindowCreate):
    return service().create_window(payload.model_dump())


@router.get("")
def list_windows(status: str | None = None, limit: int = Query(default=100, ge=1, le=500)):
    return {"items": service().list_windows(status=status, limit=limit)}


@router.get("/{code}")
def get_window(code: str):
    return service().get_window(code)


@router.get("/{code}/preview")
def preview_window(code: str):
    return service().preview(code)


@router.post("/{code}/activate")
def activate_window(code: str, payload: ActivateRequest):
    return service().activate(code, payload.actor, payload.force)


@router.post("/{code}/extend")
def extend_window(code: str, payload: ExtendRequest):
    return service().extend(code, payload.model_dump())


@router.post("/{code}/cancel")
def cancel_window(code: str, payload: CancelWindowRequest):
    return service().cancel(code, payload.actor, payload.reason)


@router.get("/{code}/progress")
def drain_progress(code: str):
    return service().progress(code)


@router.post("/{code}/checkpoints")
def submit_checkpoint(code: str, payload: CheckpointSubmit):
    return service().submit_checkpoint(code, payload.model_dump())


@router.post("/{code}/end-drain")
def end_drain(code: str, payload: EndDrainRequest):
    return service().end_drain(code, payload.actor, payload.force)


@router.get("/{code}/restore-plan")
def restore_plan(code: str):
    return service().restore_plan(code)


@router.post("/{code}/restore-batches/release")
def release_batch(code: str, payload: BatchRelease):
    return service().release_batch(code, payload.actor, payload.batch_number, payload.only_due)


@router.post("/{code}/items/{task_id}/skip")
def skip_item(code: str, task_id: int, payload: ItemSkip):
    return service().skip_item(code, task_id, payload.actor, payload.reason)


@router.post("/{code}/items/{task_id}/restore-now")
def restore_item_now(code: str, task_id: int, payload: ItemRestore):
    return service().restore_item_now(code, task_id, payload.actor, payload.reason)


@router.post("/{code}/items/{task_id}/exceptions")
def report_exception(code: str, task_id: int, payload: ExceptionReport):
    return service().report_exception(code, task_id, payload.actor, payload.note)


@router.post("/{code}/items/{task_id}/exceptions/resolve")
def resolve_exception(code: str, task_id: int, payload: ExceptionResolve):
    return service().resolve_exception(code, task_id, payload.actor, payload.note)


@router.get("/{code}/events")
def list_events(
    code: str,
    event_type: str | None = None,
    task_id: int | None = None,
    limit: int = Query(default=200, ge=1, le=1000),
):
    return service().list_events(code, event_type=event_type, task_id=task_id, limit=limit)
