from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.database import close_connection, get_connection
from app.maintenance.service import MaintenanceScheduleService

TEMPLATE = {
    "code": "solver-a",
    "name": "方程求解模板",
    "algorithm": "solver-a",
    "parameter_schema": {"iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000}},
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 3,
}


def make_window(client, code="mw-001", **overrides):
    from app.core.clock import utc_now

    start = utc_now() - timedelta(minutes=5)
    payload = {
        "code": code,
        "title": "姿态调整窗口",
        "window_type": "attitude_adjust",
        "payloads": ["solver-a"],
        "projects": [],
        "drain_strategy": "graceful",
        "restore_batch_size": 2,
        "restore_interval_seconds": 60,
        "restore_order": "committed",
        "planned_start_at": start.isoformat(),
        "planned_end_at": (start + timedelta(hours=2)).isoformat(),
        "created_by": "ops-admin",
    }
    payload.update(overrides)
    response = client.post("/api/maintenance-windows", json=payload)
    assert response.status_code == 201, response.text
    return response.json()


def submit_task(client, key, *, priority=50, project="project-a", user="researcher-1"):
    response = client.post(
        "/api/compute/tasks",
        json={
            "template_code": "solver-a",
            "project_code": project,
            "requested_by": user,
            "parameters": {"iterations": 100},
            "priority": priority,
            "idempotency_key": key,
        },
    )
    assert response.status_code == 202, response.text
    return response.json()


@pytest.fixture()
def prepared_client(client):
    created = client.post("/api/compute/templates?actor=administrator", json=TEMPLATE)
    assert created.status_code == 201, created.text
    return client


def test_preview_is_readonly_and_classifies_tasks(prepared_client):
    client = prepared_client
    queued = submit_task(client, "preview-queued-1", priority=10)
    running = submit_task(client, "preview-running-1", priority=90)
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert claimed.json()["task"]["id"] == running["id"]
    done = submit_task(client, "preview-done-1", priority=80)
    claimed_done = client.post("/api/compute/tasks/claim", json={"worker_id": "w2", "capabilities": ["solver-a"], "lease_seconds": 60})
    done_id = claimed_done.json()["task"]["id"]
    assert done_id == done["id"]
    completed = client.post(f"/api/compute/tasks/{done_id}/complete", json={"worker_id": "w2", "result": {}, "metrics": {}})
    assert completed.status_code == 200

    make_window(client)
    preview = client.get("/api/maintenance-windows/mw-001/preview").json()
    assert preview["affected_count"] == 3
    plans = {item["task_id"]: item["drain_plan"] for item in preview["affected"]}
    assert plans[queued["id"]] == "shelve"
    assert plans[running["id"]] == "checkpoint_then_shelve"
    assert plans[done["id"]] == "keep_terminal"
    assert preview["projected_restore_batches"] == [2]  # 仅搁置项参与恢复，按 batch_size=2 成一批
    # 预演不改变任务状态
    assert client.get(f"/api/compute/task-details/{queued['id']}").json()["status"] == "queued"
    assert client.get("/api/maintenance-windows/mw-001").json()["status"] == "preview"


def test_activate_shelves_queued_and_is_idempotent(prepared_client):
    client = prepared_client
    first = submit_task(client, "activate-1")
    second = submit_task(client, "activate-2")
    make_window(client)

    response = client.post("/api/maintenance-windows/mw-001/activate", json={"actor": "ops-admin"})
    assert response.status_code == 200
    detail = response.json()
    assert detail["window"]["status"] == "active"
    assert detail["drain"]["shelved"] == 2
    assert client.get(f"/api/compute/task-details/{first['id']}").json()["status"] == "shelved"

    # 重复启用幂等：不新增条目、不报错
    again = client.post("/api/maintenance-windows/mw-001/activate", json={"actor": "ops-admin"})
    assert again.status_code == 200
    assert again.json()["window"]["status"] == "active"
    assert again.json()["total_items"] == 2

    # 搁置中的任务不会被工作者领取
    claim = client.post("/api/compute/tasks/claim", json={"worker_id": "w9", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert claim.json()["task"] is None
    assert first["queue_ticket"] < second["queue_ticket"] or first["id"] < second["id"]


def test_completed_task_is_not_rolled_back(prepared_client):
    client = prepared_client
    done = submit_task(client, "terminal-1")
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
    task_id = claimed.json()["task"]["id"]
    client.post(f"/api/compute/tasks/{task_id}/complete", json={"worker_id": "w1", "result": {"v": 1}, "metrics": {}})
    make_window(client)
    client.post("/api/maintenance-windows/mw-001/activate", json={"actor": "ops-admin"})
    details = client.get(f"/api/compute/task-details/{done['id']}").json()
    assert details["status"] == "succeeded"
    assert len(details["results"]) == 1
    progress = client.get("/api/maintenance-windows/mw-001/progress").json()
    assert progress["drain"]["completed"] == 1
    assert progress["restore"]["not_required"] == 1


def test_running_task_checkpoint_then_shelve_and_claim_resumes(prepared_client):
    client = prepared_client
    running = submit_task(client, "checkpoint-1")
    client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
    make_window(client)
    client.post("/api/maintenance-windows/mw-001/activate", json={"actor": "ops-admin"})

    # 未交检查点前任务仍在运行
    progress = client.get("/api/maintenance-windows/mw-001/progress").json()
    assert [item["task_id"] for item in progress["pending_checkpoints"]] == [running["id"]]
    assert progress["drain_complete"] is False

    # 错误的工作者不能提交检查点
    forbidden = client.post(
        "/api/maintenance-windows/mw-001/checkpoints",
        json={"task_id": running["id"], "worker_id": "intruder", "checkpoint_data": {"step": 3}, "progress_percent": 30},
    )
    assert forbidden.status_code == 409

    submitted = client.post(
        "/api/maintenance-windows/mw-001/checkpoints",
        json={"task_id": running["id"], "worker_id": "w1", "checkpoint_data": {"step": 3, "offset": 42}, "progress_percent": 30.5},
    )
    assert submitted.status_code == 200
    assert client.get(f"/api/compute/task-details/{running['id']}").json()["status"] == "shelved"

    # 重复提交检查点被拒绝（条目已不在等待状态）
    duplicate = client.post(
        "/api/maintenance-windows/mw-001/checkpoints",
        json={"task_id": running["id"], "worker_id": "w1", "checkpoint_data": {"step": 4}, "progress_percent": 40},
    )
    assert duplicate.status_code == 409

    client.post("/api/maintenance-windows/mw-001/end-drain", json={"actor": "ops-admin"})
    client.post("/api/maintenance-windows/mw-001/restore-batches/release", json={"actor": "ops-admin"})
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": "w2", "capabilities": ["solver-a"], "lease_seconds": 60})
    task = claimed.json()["task"]
    assert task["id"] == running["id"]
    assert task["resume_checkpoint"]["checkpoint_data"] == {"step": 3, "offset": 42}
    assert task["resume_checkpoint"]["progress_percent"] == 30.5


def test_end_drain_blocks_when_checkpoint_missing_and_force_overrides(prepared_client):
    client = prepared_client
    submit_task(client, "force-1")
    client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
    make_window(client)
    client.post("/api/maintenance-windows/mw-001/activate", json={"actor": "ops-admin"})

    blocked = client.post("/api/maintenance-windows/mw-001/end-drain", json={"actor": "ops-admin"})
    assert blocked.status_code == 409
    forced = client.post("/api/maintenance-windows/mw-001/end-drain", json={"actor": "ops-admin", "force": True})
    assert forced.status_code == 200
    body = forced.json()
    assert body["window"]["status"] == "completed"
    assert body["skipped"][0]["skip_reason"]


def test_restore_batches_preserve_committed_order(prepared_client):
    client = prepared_client
    # 同优先级，按提交顺序领取；恢复后顺序必须保持
    t1 = submit_task(client, "order-1", priority=50)
    t2 = submit_task(client, "order-2", priority=50)
    t3 = submit_task(client, "order-3", priority=50)
    make_window(client, restore_batch_size=2, restore_interval_seconds=0)
    client.post("/api/maintenance-windows/mw-001/activate", json={"actor": "ops-admin"})
    client.post("/api/maintenance-windows/mw-001/end-drain", json={"actor": "ops-admin"})

    plan = client.get("/api/maintenance-windows/mw-001/restore-plan").json()
    assert [item["task_id"] for item in plan["order"]] == [t1["id"], t2["id"], t3["id"]]
    assert [batch["item_count"] for batch in plan["batches"]] == [2, 1]

    # 窗口期间新提交的任务，承诺序号更大
    late = submit_task(client, "order-late", priority=50)

    first = client.post("/api/maintenance-windows/mw-001/restore-batches/release", json={"actor": "ops-admin"})
    assert first.status_code == 200
    # 重复释放同一批幂等
    again = client.post("/api/maintenance-windows/mw-001/restore-batches/release", json={"actor": "ops-admin", "batch_number": 1})
    assert again.status_code == 200

    claimed_ids = []
    for _ in range(2):
        claim = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
        claimed_ids.append(claim.json()["task"]["id"])
    assert claimed_ids == [t1["id"], t2["id"]]
    # t3 仍搁置（第二批未释放），但新任务也不应越过它……t3 搁置中不可领取，late 可以领取
    late_claim = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert late_claim.json()["task"]["id"] == late["id"]

    client.post("/api/maintenance-windows/mw-001/restore-batches/release", json={"actor": "ops-admin", "batch_number": 2})
    final_claim = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert final_claim.json()["task"]["id"] == t3["id"]
    assert client.get("/api/maintenance-windows/mw-001").json()["status"] == "completed"


def test_extend_window_updates_effective_end(prepared_client):
    client = prepared_client
    submit_task(client, "extend-1")
    make_window(client, restore_interval_seconds=60)
    client.post("/api/maintenance-windows/mw-001/activate", json={"actor": "ops-admin"})
    extended = client.post(
        "/api/maintenance-windows/mw-001/extend",
        json={"actor": "ops-admin", "reason": "姿态机动延迟", "extend_seconds": 1800},
    )
    assert extended.status_code == 200
    window = extended.json()
    assert window["extended_end_at"] > window["planned_end_at"]

    # 再次延长到更早时间应被拒绝
    invalid = client.post(
        "/api/maintenance-windows/mw-001/extend",
        json={"actor": "ops-admin", "reason": "误操作", "new_end_at": window["planned_end_at"]},
    )
    assert invalid.status_code == 409

    client.post("/api/maintenance-windows/mw-001/end-drain", json={"actor": "ops-admin"})
    plan = client.get("/api/maintenance-windows/mw-001/restore-plan").json()
    # 第一批计划恢复时间 = 延长后的结束时间
    assert plan["batches"][0]["planned_at"] == window["extended_end_at"]


def test_cancel_active_window_restores_shelved_tasks(prepared_client):
    client = prepared_client
    t1 = submit_task(client, "cancel-1")
    t2 = submit_task(client, "cancel-2")
    make_window(client)
    client.post("/api/maintenance-windows/mw-001/activate", json={"actor": "ops-admin"})
    assert client.get(f"/api/compute/task-details/{t1['id']}").json()["status"] == "shelved"

    cancelled = client.post(
        "/api/maintenance-windows/mw-001/cancel",
        json={"actor": "ops-admin", "reason": "气象条件不满足，取消窗口"},
    )
    assert cancelled.status_code == 200
    assert cancelled.json()["window"]["status"] == "cancelled"
    for task_id in (t1["id"], t2["id"]):
        assert client.get(f"/api/compute/task-details/{task_id}").json()["status"] == "queued"

    # 重复取消幂等
    again = client.post(
        "/api/maintenance-windows/mw-001/cancel",
        json={"actor": "ops-admin", "reason": "重复操作"},
    )
    assert again.status_code == 200
    assert again.json()["window"]["status"] == "cancelled"

    # 已完成窗口不可取消
    make_window(client, code="mw-002", payloads=[], projects=["project-empty"])
    client.post("/api/maintenance-windows/mw-002/activate", json={"actor": "ops-admin"})
    client.post("/api/maintenance-windows/mw-002/end-drain", json={"actor": "ops-admin"})
    rejected = client.post("/api/maintenance-windows/mw-002/cancel", json={"actor": "ops-admin", "reason": "试图取消"})
    assert rejected.status_code == 409


def test_per_item_skip_exception_and_manual_restore(prepared_client):
    client = prepared_client
    queued = submit_task(client, "item-1", priority=10)
    running = submit_task(client, "item-2", priority=90)
    client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
    make_window(client, restore_batch_size=10)
    client.post("/api/maintenance-windows/mw-001/activate", json={"actor": "ops-admin"})

    # 运行中条目逐项跳过：任务保持运行、不参与恢复
    skip = client.post(
        f"/api/maintenance-windows/mw-001/items/{running['id']}/skip",
        json={"actor": "ops-admin", "reason": "该载荷改为星上自主完成"},
    )
    assert skip.status_code == 200
    assert client.get(f"/api/compute/task-details/{running['id']}").json()["status"] == "running"

    # 异常登记与解决
    report = client.post(
        f"/api/maintenance-windows/mw-001/items/{queued['id']}/exceptions",
        json={"actor": "ops-watch", "note": "下游存储不可用"},
    )
    assert report.status_code == 200
    progress = client.get("/api/maintenance-windows/mw-001/progress").json()
    assert progress["open_exceptions"][0]["task_id"] == queued["id"]
    resolve = client.post(
        f"/api/maintenance-windows/mw-001/items/{queued['id']}/exceptions/resolve",
        json={"actor": "ops-admin", "note": "存储已恢复"},
    )
    assert resolve.status_code == 200

    # 手动提前恢复单项
    early = client.post(
        f"/api/maintenance-windows/mw-001/items/{queued['id']}/restore-now",
        json={"actor": "ops-admin", "reason": "重点保障算例"},
    )
    assert early.status_code == 200
    assert client.get(f"/api/compute/task-details/{queued['id']}").json()["status"] == "queued"

    # 跳过原因可在进度中查看
    body = client.post("/api/maintenance-windows/mw-001/end-drain", json={"actor": "ops-admin"}).json()
    assert any(item["task_id"] == running["id"] and item["skip_reason"] for item in body["items"])


def test_audit_events_record_full_lifecycle(prepared_client):
    client = prepared_client
    submit_task(client, "audit-1")
    make_window(client)
    client.post("/api/maintenance-windows/mw-001/activate", json={"actor": "ops-admin"})
    client.post("/api/maintenance-windows/mw-001/end-drain", json={"actor": "ops-admin"})
    client.post("/api/maintenance-windows/mw-001/restore-batches/release", json={"actor": "ops-admin"})

    events = client.get("/api/maintenance-windows/mw-001/events").json()["items"]
    types = [event["event_type"] for event in events]
    for expected in ("window_created", "window_activated", "drain_ended", "restore_batch_released", "task_restored", "window_completed"):
        assert expected in types, expected

    filtered = client.get("/api/maintenance-windows/mw-001/events?event_type=window_activated").json()["items"]
    assert all(event["event_type"] == "window_activated" for event in filtered)


def test_state_survives_service_restart(tmp_path: Path):
    db_path = tmp_path / "restart.db"
    os.environ["TOWNSHIP_DATABASE_PATH"] = str(db_path)
    close_connection()
    try:
        clock = FrozenClock(datetime(2026, 10, 1, 2, 0, tzinfo=UTC))
        compute = ComputeOperationsService(get_connection(), clock)
        maintenance = MaintenanceScheduleService(get_connection(), clock)
        from app.database import init_db
        init_db()
        compute.create_template(TEMPLATE, "administrator")
        task = compute.submit({
            "template_code": "solver-a", "project_code": "project-a", "requested_by": "u1",
            "parameters": {"iterations": 5}, "priority": 50, "idempotency_key": "restart-0001",
        })
        maintenance.create_window({
            "code": "mw-restart", "title": "重启验证", "window_type": "other",
            "payloads": ["solver-a"], "projects": [], "drain_strategy": "graceful",
            "restore_batch_size": 5, "restore_interval_seconds": 0, "restore_order": "committed",
            "planned_start_at": clock.now(), "planned_end_at": clock.now() + timedelta(hours=1),
            "created_by": "ops-admin",
        })
        maintenance.activate("mw-restart", "ops-admin")
        assert compute.get_task(task["id"])["status"] == "shelved"

        # 模拟服务重启：丢弃线程内连接后重新打开同一数据库文件
        close_connection()
        reopened_clock = FrozenClock(datetime(2026, 10, 1, 3, 0, tzinfo=UTC))
        compute2 = ComputeOperationsService(get_connection(), reopened_clock)
        maintenance2 = MaintenanceScheduleService(get_connection(), reopened_clock)
        assert compute2.get_task(task["id"])["status"] == "shelved"
        window = maintenance2.get_window("mw-restart")
        assert window["status"] == "active"
        progress = maintenance2.progress("mw-restart")
        assert progress["total_items"] == 1

        maintenance2.end_drain("mw-restart", "ops-admin")
        maintenance2.release_batch("mw-restart", "ops-admin")
        claimed = compute2.claim("w1", ["solver-a"], 60)
        assert claimed and claimed["id"] == task["id"]
    finally:
        close_connection()
        os.environ.pop("TOWNSHIP_DATABASE_PATH", None)


def test_new_tasks_during_window_are_auto_shelved(prepared_client):
    client = prepared_client
    make_window(client)
    client.post("/api/maintenance-windows/mw-001/activate", json={"actor": "ops-admin"})
    # 窗口期间提交的受影响载荷任务，在下次对账时自动纳入排空
    late = submit_task(client, "late-1")
    progress = client.get("/api/maintenance-windows/mw-001/progress").json()
    assert progress["total_items"] == 1
    assert client.get(f"/api/compute/task-details/{late['id']}").json()["status"] == "shelved"
    events = client.get("/api/maintenance-windows/mw-001/events?event_type=task_auto_shelved").json()["items"]
    assert len(events) == 1


def test_claims_are_blocked_for_active_window_scope_before_reconcile(prepared_client):
    client = prepared_client
    # 激活时没有任何受影响任务；之后新提交，且不触发对账接口，领取仍应被拦截
    make_window(client)
    client.post("/api/maintenance-windows/mw-001/activate", json={"actor": "ops-admin"})
    submit_task(client, "gate-1")
    claim = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert claim.json()["task"] is None
    # 不在窗口范围的载荷仍可领取（此处用不匹配能力确认无任务可领后，创建其他载荷验证）
    other_template = {
        "code": "solver-b",
        "name": "其他载荷模板",
        "algorithm": "solver-b",
        "parameter_schema": {"n": {"type": "integer", "required": True, "minimum": 1}},
        "default_parameters": {},
        "max_runtime_seconds": 300,
        "max_attempts": 2,
    }
    assert client.post("/api/compute/templates?actor=administrator", json=other_template).status_code == 201
    response = client.post(
        "/api/compute/tasks",
        json={"template_code": "solver-b", "project_code": "project-a", "requested_by": "u2",
              "parameters": {"n": 1}, "priority": 10, "idempotency_key": "gate-other-1"},
    )
    other_task = response.json()
    claim_other = client.post("/api/compute/tasks/claim", json={"worker_id": "w2", "capabilities": ["solver-b"], "lease_seconds": 60})
    assert claim_other.json()["task"]["id"] == other_task["id"]


def test_activate_before_planned_start_requires_force(prepared_client):
    client = prepared_client
    submit_task(client, "early-activate-1")
    future_start = datetime.now(UTC) + timedelta(hours=1)
    make_window(client, code="mw-future", planned_start_at=future_start.isoformat(),
                planned_end_at=(future_start + timedelta(hours=2)).isoformat())
    rejected = client.post("/api/maintenance-windows/mw-future/activate", json={"actor": "ops-admin"})
    assert rejected.status_code == 409
    forced = client.post("/api/maintenance-windows/mw-future/activate", json={"actor": "ops-admin", "force": True})
    assert forced.status_code == 200
    assert forced.json()["window"]["status"] == "active"


def test_drain_strategies_change_behavior(prepared_client):
    client = prepared_client

    # checkpoint_only：排队任务保持 queued，但活动窗口期间领取被冻结
    queued = submit_task(client, "strategy-cp-1")
    make_window(client, code="mw-cp", drain_strategy="checkpoint_only")
    activated = client.post("/api/maintenance-windows/mw-cp/activate", json={"actor": "ops-admin"})
    assert activated.status_code == 200
    assert client.get(f"/api/compute/task-details/{queued['id']}").json()["status"] == "queued"
    assert client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60}).json()["task"] is None
    client.post("/api/maintenance-windows/mw-cp/end-drain", json={"actor": "ops-admin"})
    # 窗口结束后排队任务自然可领，无需恢复
    claim = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert claim.json()["task"]["id"] == queued["id"]

    # immediate：运行中任务收到取消信号但仍可交检查点后搁置
    running = submit_task(client, "strategy-imm-1", priority=90)
    claim_run = client.post("/api/compute/tasks/claim", json={"worker_id": "w2", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert claim_run.json()["task"]["id"] == running["id"]
    make_window(client, code="mw-imm", drain_strategy="immediate")
    client.post("/api/maintenance-windows/mw-imm/activate", json={"actor": "ops-admin"})
    assert client.get(f"/api/compute/task-details/{running['id']}").json()["status"] == "cancel_requested"
    cp = client.post(
        "/api/maintenance-windows/mw-imm/checkpoints",
        json={"task_id": running["id"], "worker_id": "w2", "checkpoint_data": {"i": 9}, "progress_percent": 80},
    )
    assert cp.status_code == 200
    assert client.get(f"/api/compute/task-details/{running['id']}").json()["status"] == "shelved"


def test_window_requires_at_least_one_scope(prepared_client):
    start = datetime(2026, 10, 1, 2, 0, tzinfo=UTC)
    resp = prepared_client.post(
        "/api/maintenance-windows",
        json={
            "code": "mw-empty", "title": "空范围窗口", "window_type": "other",
            "payloads": [], "projects": [], "drain_strategy": "graceful",
            "restore_batch_size": 10, "restore_interval_seconds": 0, "restore_order": "committed",
            "planned_start_at": start.isoformat(),
            "planned_end_at": (start + timedelta(hours=1)).isoformat(),
            "created_by": "ops-admin",
        },
    )
    assert resp.status_code == 422
