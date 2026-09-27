from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.core.errors import ConflictError
from app.database import get_connection
from app.scheduling.service import MaintenanceSchedulingService

TEMPLATE = {
    "code": "solver-mw",
    "name": "维护排程模板",
    "algorithm": "solver-mw",
    "parameter_schema": {"steps": {"type": "integer", "required": True, "minimum": 1}},
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
}


def _payload(key: str, *, user: str = "researcher-1", project: str = "payload-a", priority: int = 50) -> dict:
    return {
        "template_code": "solver-mw",
        "project_code": project,
        "requested_by": user,
        "parameters": {"steps": 10},
        "priority": priority,
        "idempotency_key": key,
    }


def _window(code: str, *, start: datetime, end: datetime, strategy: str = "graceful", payloads=None, batch_size: int = 2, interval: int = 60) -> dict:
    return {
        "code": code,
        "title": "姿态调整窗口",
        "window_type": "attitude_adjust",
        "payloads": payloads or ["payload-a"],
        "drain_strategy": strategy,
        "starts_at": start.isoformat(),
        "ends_at": end.isoformat(),
        "grace_period_seconds": 0,
        "restore_batch_size": batch_size,
        "restore_interval_seconds": interval,
        "created_by": "ops-1",
    }


def _services(clock: FrozenClock) -> tuple[ComputeOperationsService, MaintenanceSchedulingService]:
    compute = ComputeOperationsService(get_connection(), clock)
    compute.create_template(TEMPLATE, "administrator")
    return compute, MaintenanceSchedulingService(get_connection(), clock)


def test_preview_plan_does_not_mutate(client):
    clock = FrozenClock(datetime(2026, 9, 27, 1, 0, tzinfo=UTC))
    compute, scheduling = _services(clock)
    compute.submit(_payload("mw-preview-1"))
    start = clock.now() + timedelta(hours=1)
    scheduling.create_window(_window("mw-preview", start=start, end=start + timedelta(hours=2)))
    preview = scheduling.preview_window("mw-preview")
    assert preview["fresh_preview"]["will_hold"] == 1
    assert compute.list_tasks(status="queued")[0]["status"] == "queued"
    assert scheduling.list_windows()[0]["status"] == "planned"


def test_activate_drains_queued_and_is_idempotent(client):
    clock = FrozenClock(datetime(2026, 9, 27, 2, 0, tzinfo=UTC))
    compute, scheduling = _services(clock)
    queued = compute.submit(_payload("mw-act-1", priority=10))
    run = compute.submit(_payload("mw-act-2", priority=90))
    compute.submit(_payload("mw-act-3", priority=50))
    claimed = compute.claim("worker-1", ["solver-mw"], 600)
    assert claimed["id"] == run["id"]
    compute.submit(_payload("mw-act-4", priority=50))
    done = compute.claim("worker-2", ["solver-mw"], 600)
    assert done["id"] == 3
    compute.complete(done["id"], "worker-2", {"ok": True}, {})

    start = clock.now() - timedelta(minutes=5)
    scheduling.create_window(_window("mw-act", start=start, end=clock.now() + timedelta(hours=1)))
    activated = scheduling.activate("mw-act", "ops-1")
    again = scheduling.activate("mw-act", "ops-1")
    assert again.get("idempotent") is True

    states = {item["task_id"]: item["state"] for item in activated["items"]}
    roles = {item["task_id"]: item["item_role"] for item in activated["items"]}
    assert states[queued["id"]] == "drained"
    assert states[run["id"]] == "checkpointing"
    assert states[done["id"]] == "skipped"
    assert roles[done["id"]] == "succeeded"

    assert compute.get_task(queued["id"])["status"] == "held"
    assert compute.get_task(run["id"])["status"] == "running"
    assert compute.get_task(done["id"])["status"] == "succeeded"
    assert compute.claim("worker-3", ["solver-mw"], 600) is None


def test_checkpoint_then_batched_restore_preserves_priority_order(client):
    clock = FrozenClock(datetime(2026, 9, 27, 3, 0, tzinfo=UTC))
    compute, scheduling = _services(clock)
    compute.submit(_payload("mw-rest-1", priority=10))
    run = compute.submit(_payload("mw-rest-2", priority=90))
    compute.submit(_payload("mw-rest-3", priority=40))
    compute.submit(_payload("mw-rest-4", priority=50))
    claimed = compute.claim("worker-1", ["solver-mw"], 600)
    assert claimed["id"] == run["id"]

    start = clock.now() - timedelta(minutes=10)
    end = clock.now() + timedelta(minutes=50)
    scheduling.create_window(_window("mw-rest", start=start, end=end, batch_size=2))
    scheduling.activate("mw-rest", "ops-1")

    running_item = next(item for item in scheduling.get_window("mw-rest")["items"] if item["task_id"] == run["id"])
    checkpointed = scheduling.report_checkpoint(running_item["id"], {"actor": "worker-1", "checkpoint": {"step": 7}})
    assert checkpointed["state"] == "drained"
    assert checkpointed["checkpoint"] == {"step": 7}
    assert compute.get_task(run["id"])["status"] == "held"

    with pytest.raises(ConflictError, match="尚未结束"):
        scheduling.begin_restore("mw-rest", "ops-1")

    clock.advance(minutes=55)
    restoring = scheduling.begin_restore("mw-rest", "ops-1")
    assert restoring["status"] == "recovering"
    order = restoring["restore_order"]
    assert [entry["task_id"] for entry in order] == [run["id"], 4, 3, 1]
    assert compute.get_task(run["id"])["status"] == "queued"
    assert compute.get_task(1)["status"] == "held"
    assert scheduling.begin_restore("mw-rest", "ops-1").get("idempotent") is True

    clock.advance(seconds=61)
    result = scheduling.run_due_releases()
    assert result["released_batch_ids"]
    assert compute.get_task(1)["status"] == "queued"
    final = scheduling.get_window("mw-rest")
    assert final["status"] == "completed"
    assert final["progress"]["restored"] == 4
    picked = compute.claim("worker-9", ["solver-mw"], 600)
    assert picked["id"] == run["id"]


def test_extend_blocks_restore_before_new_end(client):
    clock = FrozenClock(datetime(2026, 9, 27, 5, 0, tzinfo=UTC))
    compute, scheduling = _services(clock)
    compute.submit(_payload("mw-ext2-1"))
    start = clock.now() - timedelta(minutes=5)
    end = clock.now() + timedelta(minutes=30)
    scheduling.create_window(_window("mw-ext2", start=start, end=end, batch_size=10))
    scheduling.activate("mw-ext2", "ops-1")
    extended = scheduling.extend("mw-ext2", {"actor": "ops-1", "reason": "延期", "extra_seconds": 1800, "new_ends_at": None})
    assert extended["status"] == "extended"
    assert extended["ends_at"] > end.isoformat()
    clock.advance(minutes=31)
    with pytest.raises(ConflictError, match="尚未结束"):
        scheduling.begin_restore("mw-ext2", "ops-1")
    clock.advance(minutes=30)
    restored_window = scheduling.begin_restore("mw-ext2", "ops-1")
    assert restored_window["status"] in {"recovering", "completed"}
    assert compute.get_task(1)["status"] == "queued"


def test_cancel_releases_held_and_is_idempotent(client):
    clock = FrozenClock(datetime(2026, 9, 27, 6, 0, tzinfo=UTC))
    compute, scheduling = _services(clock)
    task = compute.submit(_payload("mw-cancel-1"))
    start = clock.now() - timedelta(minutes=5)
    scheduling.create_window(_window("mw-cancel", start=start, end=clock.now() + timedelta(hours=1), batch_size=10))
    scheduling.activate("mw-cancel", "ops-1")
    assert compute.get_task(task["id"])["status"] == "held"
    cancelled = scheduling.cancel("mw-cancel", {"actor": "ops-1", "reason": "窗口取消"})
    assert cancelled["status"] == "cancelled"
    assert compute.get_task(task["id"])["status"] == "queued"
    assert scheduling.cancel("mw-cancel", {"actor": "ops-1", "reason": "重复取消"}).get("idempotent") is True


def test_item_level_skip_manual_restore_and_failures(client):
    clock = FrozenClock(datetime(2026, 9, 27, 7, 0, tzinfo=UTC))
    compute, scheduling = _services(clock)
    t1 = compute.submit(_payload("mw-item-1", priority=50))
    t2 = compute.submit(_payload("mw-item-2", priority=50))
    t3 = compute.submit(_payload("mw-item-3", priority=50))
    claimed = compute.claim("worker-1", ["solver-mw"], 600)
    assert claimed["id"] == t1["id"]
    start = clock.now() - timedelta(minutes=5)
    end = clock.now() + timedelta(minutes=30)
    scheduling.create_window(_window("mw-item", start=start, end=end, batch_size=1, interval=120))
    scheduling.activate("mw-item", "ops-1")
    items = {item["task_id"]: item for item in scheduling.get_window("mw-item")["items"]}

    skipped = scheduling.skip_item(items[t2["id"]]["id"], {"actor": "ops-2", "reason": "载荷豁免"})
    assert skipped["state"] == "skipped"
    assert skipped["skip_reason"] == "载荷豁免"
    assert compute.get_task(t2["id"])["status"] == "queued"

    scheduling.report_checkpoint_failure(items[t1["id"]]["id"], {"actor": "worker-1", "reason": "写入失败"})
    assert scheduling.get_item(items[t1["id"]]["id"])["state"] == "checkpoint_failed"
    forced = scheduling.force_checkpoint(items[t1["id"]]["id"], {"actor": "ops-2", "reason": "人工强制", "checkpoint": {"step": 3}})
    assert forced["state"] == "drained"

    clock.advance(minutes=31)
    restoring = scheduling.begin_restore("mw-item", "ops-1")
    assert restoring["status"] == "recovering"
    # 批次 1 立即释放 t1；t3 在第二批（60 秒后），仍为 drained
    items_now = {item["task_id"]: item for item in scheduling.get_window("mw-item")["items"]}
    assert items_now[t1["id"]]["state"] == "restored"
    assert items_now[t3["id"]]["state"] == "drained"
    failure = scheduling.report_restore_failure(items_now[t3["id"]]["id"], {"actor": "worker-1", "reason": "存储暂不可用"})
    assert failure["state"] == "failed"
    restored = scheduling.restore_item(items_now[t3["id"]]["id"], {"actor": "ops-2", "reason": "存储已恢复"})
    assert restored["state"] == "restored"
    assert compute.get_task(t3["id"])["status"] == "queued"

    clock.advance(seconds=121)
    scheduling.run_due_releases()
    final = scheduling.get_window("mw-item")
    assert final["status"] == "completed"
    assert "载荷豁免" in final["skip_reasons"]


def test_pause_new_strategy_keeps_running_unaffected(client):
    clock = FrozenClock(datetime(2026, 9, 27, 8, 0, tzinfo=UTC))
    compute, scheduling = _services(clock)
    queued = compute.submit(_payload("mw-pause-1"))
    running = compute.submit(_payload("mw-pause-2", priority=90))
    compute.claim("worker-1", ["solver-mw"], 600)
    start = clock.now() - timedelta(minutes=5)
    scheduling.create_window(_window("mw-pause", start=start, end=clock.now() + timedelta(hours=1), strategy="pause_new", batch_size=10))
    activated = scheduling.activate("mw-pause", "ops-1")
    states = {item["task_id"]: item["state"] for item in activated["items"]}
    assert states[queued["id"]] == "drained"
    assert states[running["id"]] == "skipped"
    assert compute.get_task(running["id"])["status"] == "running"


def test_audit_events_and_restart_persistence(client):
    clock = FrozenClock(datetime(2026, 9, 27, 9, 0, tzinfo=UTC))
    compute, scheduling = _services(clock)
    compute.submit(_payload("mw-audit-1"))
    start = clock.now() - timedelta(minutes=5)
    scheduling.create_window(_window("mw-audit", start=start, end=clock.now() + timedelta(hours=1), batch_size=10))
    scheduling.activate("mw-audit", "ops-1")
    scheduling.cancel("mw-audit", {"actor": "ops-1", "reason": "取消"})

    events = scheduling.list_audit("mw-audit")["items"]
    actions = [event["action"] for event in events]
    assert {"window.create", "window.activate", "window.cancel"} <= set(actions)

    reloaded = MaintenanceSchedulingService(get_connection(), clock)
    window = reloaded.get_window("mw-audit")
    assert window["status"] == "cancelled"
    assert window["progress"]["total"] == 1
    assert len(reloaded.list_audit("mw-audit")["items"]) == len(events)


def test_force_begin_restore_with_checkpoint_failure(client):
    clock = FrozenClock(datetime(2026, 9, 27, 10, 0, tzinfo=UTC))
    compute, scheduling = _services(clock)
    compute.submit(_payload("mw-force-1"))
    run = compute.submit(_payload("mw-force-2", priority=90))
    compute.claim("worker-1", ["solver-mw"], 600)
    start = clock.now() - timedelta(minutes=5)
    end = clock.now() + timedelta(minutes=10)
    scheduling.create_window(_window("mw-force", start=start, end=end, batch_size=10))
    scheduling.activate("mw-force", "ops-1")
    items = {item["task_id"]: item for item in scheduling.get_window("mw-force")["items"]}
    scheduling.report_checkpoint_failure(items[run["id"]]["id"], {"actor": "worker-1", "reason": "无法落盘"})
    clock.advance(minutes=11)
    with pytest.raises(ConflictError, match="检查点异常"):
        scheduling.begin_restore("mw-force", "ops-1")
    forced = scheduling.begin_restore("mw-force", "ops-1", force=True)
    assert forced["status"] == "recovering"
    detail = scheduling.get_window("mw-force")
    assert detail["progress"]["awaiting_checkpoint"] == 1
    assert detail["status"] == "recovering"


def test_completed_task_during_window_is_not_rolled_back(client):
    clock = FrozenClock(datetime(2026, 9, 27, 11, 0, tzinfo=UTC))
    compute, scheduling = _services(clock)
    run = compute.submit(_payload("mw-done-1", priority=90))
    claimed = compute.claim("worker-1", ["solver-mw"], 600)
    start = clock.now() - timedelta(minutes=5)
    end = clock.now() + timedelta(minutes=30)
    scheduling.create_window(_window("mw-done", start=start, end=end, batch_size=10))
    scheduling.activate("mw-done", "ops-1")
    item = next(item for item in scheduling.get_window("mw-done")["items"] if item["task_id"] == run["id"])
    assert item["state"] == "checkpointing"
    # 工作者在窗口期内完成任务：协调时识别为自然完成，不可回滚
    compute.complete(claimed["id"], "worker-1", {"value": 1}, {})
    clock.advance(minutes=31)
    detail = scheduling.get_window("mw-done")
    item_after = next(item for item in detail["items"] if item["task_id"] == run["id"])
    assert item_after["state"] == "skipped"
    assert item_after["skip_reason"] == "completed_naturally:succeeded"
    assert compute.get_task(run["id"])["status"] == "succeeded"
    assert detail["status"] == "completed"
