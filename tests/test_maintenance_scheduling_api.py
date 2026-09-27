from __future__ import annotations

from datetime import UTC, datetime, timedelta

PREFIX = "/api/maintenance-scheduling"


def _template() -> dict:
    return {
        "code": "solver-api",
        "name": "接口排程模板",
        "algorithm": "solver-api",
        "parameter_schema": {"steps": {"type": "integer", "required": True, "minimum": 1}},
        "default_parameters": {},
        "max_runtime_seconds": 300,
        "max_attempts": 2,
    }


def _submit(client, key: str, priority: int = 50) -> int:
    response = client.post(
        "/api/compute/tasks",
        json={
            "template_code": "solver-api",
            "project_code": "sat-payload-1",
            "requested_by": "r-1",
            "parameters": {"steps": 3},
            "priority": priority,
            "idempotency_key": key,
        },
    )
    assert response.status_code == 202, response.text
    return response.json()["id"]


def test_scheduling_http_lifecycle(client):
    assert client.post("/api/compute/templates?actor=ops", json=_template()).status_code == 201
    task_id = _submit(client, "api-mw-000001", priority=40)

    start = datetime.now(UTC) - timedelta(minutes=5)
    end = datetime.now(UTC) + timedelta(hours=1)
    window_payload = {
        "code": "attitude-20260927",
        "title": "姿态调整",
        "window_type": "attitude_adjust",
        "payloads": ["sat-payload-1"],
        "drain_strategy": "graceful",
        "starts_at": start.isoformat(),
        "ends_at": end.isoformat(),
        "grace_period_seconds": 0,
        "restore_batch_size": 10,
        "restore_interval_seconds": 0,
        "created_by": "ops-1",
    }

    created = client.post(f"{PREFIX}/windows", json=window_payload)
    assert created.status_code == 201, created.text
    assert created.json()["status"] == "planned"

    preview = client.get(f"{PREFIX}/windows/attitude-20260927/preview")
    assert preview.status_code == 200
    assert preview.json()["fresh_preview"]["will_hold"] == 1

    duplicate = client.post(f"{PREFIX}/windows", json=window_payload)
    assert duplicate.status_code == 409

    activated = client.post(f"{PREFIX}/windows/attitude-20260927/activate", json={"actor": "ops-1"})
    assert activated.status_code == 200
    assert activated.json()["progress"]["drained"] == 1
    # 重复启用幂等
    again = client.post(f"{PREFIX}/windows/attitude-20260927/activate", json={"actor": "ops-1"})
    assert again.status_code == 200 and again.json().get("idempotent") is True

    detail = client.get(f"{PREFIX}/windows/attitude-20260927")
    assert detail.status_code == 200
    body = detail.json()
    assert body["items"][0]["task"]["status"] == "held"
    assert body["restore_order"] == []

    # 延长
    extended = client.post(
        f"{PREFIX}/windows/attitude-20260927/extend",
        json={"actor": "ops-1", "reason": "机械臂展开延迟", "extra_seconds": 1800},
    )
    assert extended.status_code == 200 and extended.json()["status"] == "extended"

    # 恢复：此时窗口结束时间未到，应被拒绝
    early = client.post(f"{PREFIX}/windows/attitude-20260927/restore/begin", json={"actor": "ops-1"})
    assert early.status_code == 409

    # 取消会释放挂起任务
    cancelled = client.post(
        f"{PREFIX}/windows/attitude-20260927/cancel",
        json={"actor": "ops-1", "reason": "窗口撤销"},
    )
    assert cancelled.status_code == 200
    task = client.get(f"/api/compute/task-details/{task_id}").json()
    assert task["status"] == "queued"
    cancel_again = client.post(
        f"{PREFIX}/windows/attitude-20260927/cancel", json={"actor": "ops-1", "reason": "重复取消"}
    )
    assert cancel_again.json().get("idempotent") is True

    audit = client.get(f"{PREFIX}/audit-events?code=attitude-20260927")
    actions = [event["action"] for event in audit.json()["items"]]
    assert "window.create" in actions
    assert "window.activate" in actions
    assert "window.extend" in actions
    assert "window.cancel" in actions


def test_scheduling_http_checkpoint_and_batches(client):
    assert client.post("/api/compute/templates?actor=ops", json=_template()).status_code == 201
    queued_id = _submit(client, "api-mw-000002", priority=20)
    run_id = _submit(client, "api-mw-000003", priority=90)
    claimed = client.post(
        "/api/compute/tasks/claim",
        json={"worker_id": "w-1", "capabilities": ["solver-api"], "lease_seconds": 600},
    )
    assert claimed.json()["task"]["id"] == run_id

    start = datetime.now(UTC) - timedelta(minutes=5)
    end = datetime.now(UTC) + timedelta(minutes=30)
    response = client.post(
        f"{PREFIX}/windows",
        json={
            "code": "radiator-20260927",
            "title": "散热器维护",
            "window_type": "radiator_maintenance",
            "payloads": ["sat-payload-1"],
            "drain_strategy": "graceful",
            "starts_at": start.isoformat(),
            "ends_at": end.isoformat(),
            "grace_period_seconds": 0,
            "restore_batch_size": 1,
            "restore_interval_seconds": 0,
            "created_by": "ops-1",
        },
    )
    assert response.status_code == 201
    client.post(f"{PREFIX}/windows/radiator-20260927/activate", json={"actor": "ops-1"})

    items = client.get(f"{PREFIX}/windows/radiator-20260927").json()["items"]
    running_item = next(item for item in items if item["task_id"] == run_id)
    queued_item = next(item for item in items if item["task_id"] == queued_id)

    failed = client.post(
        f"{PREFIX}/items/{running_item['id']}/checkpoint/failed",
        json={"actor": "w-1", "reason": "磁盘满"},
    )
    assert failed.status_code == 200 and failed.json()["state"] == "checkpoint_failed"
    forced = client.post(
        f"{PREFIX}/items/{running_item['id']}/checkpoint/force",
        json={"actor": "ops-1", "reason": "已清理磁盘", "checkpoint": {"offset": 42}},
    )
    assert forced.status_code == 200 and forced.json()["state"] == "drained"

    # 逐项跳过排队任务：记录原因，任务按原承诺回队
    skipped = client.post(f"{PREFIX}/items/{queued_item['id']}/skip", json={"actor": "ops-1", "reason": "该载荷豁免"})
    assert skipped.status_code == 200 and skipped.json()["skip_reason"] == "该载荷豁免"

    # 窗口期未结束不能恢复
    assert client.post(f"{PREFIX}/windows/radiator-20260927/restore/begin", json={"actor": "ops-1"}).status_code == 409

    # 手工释放不在支持范围：时间到了后由调度入口释放；这里直接强制开始
    forced_begin = client.post(
        f"{PREFIX}/windows/radiator-20260927/restore/begin?force=true", json={"actor": "ops-1"}
    )
    # 窗口仍未结束，force 放行
    assert forced_begin.status_code == 200

    final = client.get(f"{PREFIX}/windows/radiator-20260927").json()
    assert final["status"] == "completed"
    task = client.get(f"/api/compute/task-details/{run_id}").json()
    assert task["status"] == "queued"
    assert "该载荷豁免" in final["skip_reasons"]

    audit = client.get(f"{PREFIX}/audit-events?code=radiator-20260927")
    assert any(event["action"] == "item.checkpoint_forced" for event in audit.json()["items"])
