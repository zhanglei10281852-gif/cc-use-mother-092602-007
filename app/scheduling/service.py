from __future__ import annotations

import json
import sqlite3
from typing import Any

from datetime import timedelta

from app.core.clock import Clock, SystemClock, from_storage, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import get_connection, transaction
from app.scheduling.repository import SchedulingRepository

ACTIVE_STATES = {"active", "extended"}
DRAIN_PHASE_STATES = {"active", "extended", "recovering"}
TERMINAL_TASK_STATES = {"succeeded", "failed", "cancelled"}
RUNNING_TASK_STATES = {"running", "cancel_requested"}


class MaintenanceSchedulingService:
    """维护窗口的预演、启用、排空、延长、取消、恢复批次与逐项异常处理。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = SchedulingRepository(self.connection)

    # ---------- 预演与创建 ----------
    def create_window(self, payload: dict[str, Any]) -> dict[str, Any]:
        starts_at = self._parse_time(payload["starts_at"], "starts_at")
        ends_at = self._parse_time(payload["ends_at"], "ends_at")
        if ends_at <= starts_at:
            raise ValidationError("窗口结束时间必须晚于开始时间")
        payload = {**payload, "starts_at": to_storage(starts_at), "ends_at": to_storage(ends_at)}
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = SchedulingRepository(connection)
            if repository.window_by_code(payload["code"]):
                raise ConflictError("维护窗口编码已存在")
            plan = self._build_plan(connection, payload, now)
            window = repository.create_window(payload=payload, plan=plan, now=now)
            repository.add_audit(
                window_id=window["id"], action="window.create", actor=payload["created_by"],
                outcome="success", detail={"code": payload["code"], "plan": plan}, now=now,
            )
            return self._window_detail(connection, window)

    def preview_window(self, code: str) -> dict[str, Any]:
        with transaction(immediate=True) as connection:
            window = self._require_window(connection, code)
            serialized = self._serialize_window(window)
            plan_input = {
                "payloads": serialized["payloads"],
                "drain_strategy": serialized["drain_strategy"],
                "restore_batch_size": serialized["plan"].get("restore_batch_size", 50),
                "restore_interval_seconds": serialized["plan"].get("restore_interval_seconds", 0),
            }
            plan = self._build_plan(connection, plan_input, to_storage(self.clock.now()))
            return {"window": serialized, "fresh_preview": plan}

    # ---------- 查询 ----------
    def list_windows(self, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return [self._serialize_window(row) for row in self.repository.list_windows(status, max(1, min(limit, 500)))]

    def get_window(self, code: str) -> dict[str, Any]:
        with transaction(immediate=True) as connection:
            window = self._require_window(connection, code)
            self._reconcile(connection, window)
            return self._window_detail(connection, SchedulingRepository(connection).window_by_id(window["id"]))

    def get_item(self, item_id: int) -> dict[str, Any]:
        row = self.repository.item_by_id(item_id)
        if row is None:
            raise NotFoundError("排空项不存在")
        return self._serialize_item(self.connection, row)

    def list_audit(self, code: str | None = None, limit: int = 100) -> dict[str, Any]:
        window_id = None
        if code:
            window = self.repository.window_by_code(code)
            if window is None:
                raise NotFoundError("维护窗口不存在")
            window_id = window["id"]
        events = self.repository.list_audit(window_id, max(1, min(limit, 1000)))
        for event in events:
            event["detail"] = json.loads(event["detail_json"] or "{}")
        return {"items": events}

    # ---------- 正式启用（幂等） ----------
    def activate(self, code: str, actor: str) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = SchedulingRepository(connection)
            window = self._require_window(connection, code)
            if window["status"] in ACTIVE_STATES | {"recovering", "completed"}:
                return {**self._window_detail(connection, window), "idempotent": True}
            if window["status"] != "planned":
                raise ConflictError(f"窗口当前状态 {window['status']} 不允许启用")
            self._snapshot_drain(connection, window, now)
            repository.update_window_status(window["id"], "active", now, committed_at=now)
            repository.add_audit(window_id=window["id"], action="window.activate", actor=actor, outcome="success", detail={}, now=now)
            return self._window_detail(connection, repository.window_by_id(window["id"]))

    # ---------- 临时延长 ----------
    def extend(self, code: str, payload: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = SchedulingRepository(connection)
            window = self._require_window(connection, code)
            if window["status"] not in ACTIVE_STATES:
                raise ConflictError(f"窗口当前状态 {window['status']} 不允许延长")
            current_end = from_storage(window["ends_at"])
            if payload.get("new_ends_at"):
                new_end = self._parse_time(payload["new_ends_at"], "new_ends_at")
            else:
                new_end = current_end + timedelta(seconds=payload["extra_seconds"])
            if new_end <= current_end:
                raise ValidationError("延长后的结束时间必须晚于当前结束时间")
            ends_text = to_storage(new_end)
            repository.update_window_status(window["id"], "extended", now, ends_at=ends_text)
            repository.add_audit(
                window_id=window["id"], action="window.extend", actor=payload["actor"], outcome="success",
                detail={"reason": payload["reason"], "previous_ends_at": window["ends_at"], "new_ends_at": ends_text}, now=now,
            )
            return self._window_detail(connection, repository.window_by_id(window["id"]))

    # ---------- 取消 ----------
    def cancel(self, code: str, payload: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = SchedulingRepository(connection)
            window = self._require_window(connection, code)
            if window["status"] in {"completed", "cancelled"}:
                return {**self._window_detail(connection, window), "idempotent": True}
            if window["status"] == "recovering":
                raise ConflictError("窗口已进入恢复阶段，不能取消")
            restored = 0
            if window["status"] in ACTIVE_STATES:
                restored = self._release_all_held(connection, window, now, skip_reason="window_cancelled")
            repository.update_window_status(window["id"], "cancelled", now, cancelled_at=now)
            repository.add_audit(
                window_id=window["id"], action="window.cancel", actor=payload["actor"], outcome="success",
                detail={"reason": payload["reason"], "restored_items": restored}, now=now,
            )
            return self._window_detail(connection, repository.window_by_id(window["id"]))

    # ---------- 检查点 ----------
    def report_checkpoint(self, item_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        return self._checkpoint(item_id, payload, forced=False)

    def force_checkpoint(self, item_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        return self._checkpoint(item_id, payload, forced=True)

    def report_checkpoint_failure(self, item_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = SchedulingRepository(connection)
            item = self._require_item(connection, item_id)
            if item["state"] not in {"checkpointing", "checkpoint_failed"}:
                raise ConflictError(f"排空项状态 {item['state']} 不允许上报检查点失败")
            repository.update_item(item_id, now, state="checkpoint_failed", exception_note=payload["reason"][:1000], updated_by=payload["actor"])
            repository.add_audit(
                window_id=item["window_id"], action="item.checkpoint_failed", actor=payload["actor"], outcome="failure",
                detail={"item_id": item_id, "task_id": item["task_id"], "reason": payload["reason"]}, now=now,
            )
            return self._serialize_item(connection, repository.item_by_id(item_id))

    # ---------- 逐项异常：跳过 / 单独恢复 / 恢复失败 ----------
    def skip_item(self, item_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = SchedulingRepository(connection)
            item = self._require_item(connection, item_id)
            if item["state"] in {"restored", "skipped"}:
                raise ConflictError(f"排空项已处于终态 {item['state']}")
            self._detach_task(connection, item, reason="item_skipped", now=now)
            repository.update_item(item_id, now, state="skipped", skip_reason=payload["reason"][:1000], exception_note=payload["reason"][:1000], updated_by=payload["actor"])
            repository.add_audit(
                window_id=item["window_id"], action="item.skip", actor=payload["actor"], outcome="success",
                detail={"item_id": item_id, "task_id": item["task_id"], "reason": payload["reason"]}, now=now,
            )
            return self._serialize_item(connection, repository.item_by_id(item_id))

    def restore_item(self, item_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = SchedulingRepository(connection)
            item = self._require_item(connection, item_id)
            window = repository.window_by_id(item["window_id"])
            if window["status"] not in DRAIN_PHASE_STATES:
                raise ConflictError("窗口未进入可恢复阶段")
            if item["state"] not in {"drained", "failed"}:
                raise ConflictError(f"排空项状态 {item['state']} 不允许人工恢复")
            self._restore_one(connection, item, now)
            repository.update_item(item_id, now, state="restored", exception_note=payload["reason"][:1000], updated_by=payload["actor"])
            repository.add_audit(
                window_id=item["window_id"], action="item.restore_manual", actor=payload["actor"], outcome="success",
                detail={"item_id": item_id, "task_id": item["task_id"], "reason": payload["reason"]}, now=now,
            )
            return self._serialize_item(connection, repository.item_by_id(item_id))

    def report_restore_failure(self, item_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = SchedulingRepository(connection)
            item = self._require_item(connection, item_id)
            if item["state"] not in {"restoring", "drained"}:
                raise ConflictError(f"排空项状态 {item['state']} 不允许上报恢复失败")
            repository.update_item(item_id, now, state="failed", exception_note=payload["reason"][:1000], updated_by=payload["actor"])
            repository.add_audit(
                window_id=item["window_id"], action="item.restore_failed", actor=payload["actor"], outcome="failure",
                detail={"item_id": item_id, "task_id": item["task_id"], "reason": payload["reason"]}, now=now,
            )
            return self._serialize_item(connection, repository.item_by_id(item_id))

    # ---------- 恢复批次 ----------
    def begin_restore(self, code: str, actor: str, *, force: bool = False) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = SchedulingRepository(connection)
            window = self._require_window(connection, code)
            if window["status"] == "recovering":
                return {**self._window_detail(connection, window), "idempotent": True}
            if window["status"] not in ACTIVE_STATES:
                raise ConflictError(f"窗口当前状态 {window['status']} 不允许开始恢复")
            self._reconcile_checkpointing(connection, window, now)
            if now_value < from_storage(window["ends_at"]) and not force:
                raise ConflictError("窗口尚未结束，不能开始恢复")
            pending = repository.items_in_states(window["id"], ["pending", "checkpointing", "checkpoint_failed"])
            if pending and not force:
                raise ConflictError("仍有任务未完成排空或检查点异常，可确认后强制开始恢复", context={"pending_items": [item["id"] for item in pending]})
            # 强制提前恢复时立即开始放行，否则以窗口结束时间为基准
            base = now if force and now_value < from_storage(window["ends_at"]) else window["ends_at"]
            self._build_restore_batches(connection, window, now, base=base)
            repository.update_window_status(window["id"], "recovering", now)
            repository.add_audit(
                window_id=window["id"], action="restore.begin", actor=actor, outcome="success",
                detail={"forced": force, "pending_items": [item["id"] for item in pending]}, now=now,
            )
            self._release_due_batches(connection, repository.window_by_id(window["id"]), now)
            self._maybe_complete(connection, repository.window_by_id(window["id"]), now, actor)
            return self._window_detail(connection, repository.window_by_id(window["id"]))

    def release_batch(self, batch_id: int, actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = SchedulingRepository(connection)
            batch = repository.batch_by_id(batch_id)
            if batch is None:
                raise NotFoundError("恢复批次不存在")
            if batch["status"] == "released":
                return {"idempotent": True, "batch": dict(batch)}
            self._release_batch(connection, batch, now)
            repository.add_audit(
                window_id=batch["window_id"], action="batch.release_manual", actor=actor, outcome="success",
                detail={"batch_id": batch_id, "batch_no": batch["batch_no"]}, now=now,
            )
            self._maybe_complete(connection, repository.window_by_id(batch["window_id"]), now, actor)
            return {"batch": dict(repository.batch_by_id(batch_id))}

    def run_due_releases(self, actor: str = "scheduler") -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = SchedulingRepository(connection)
            windows = connection.execute(
                "SELECT * FROM maintenance_windows WHERE status IN ('active','extended','recovering')"
            ).fetchall()
            for window in windows:
                self._reconcile(connection, window)
            released = [
                row["id"]
                for row in connection.execute(
                    "SELECT id FROM maintenance_restore_batches WHERE released_at=? ORDER BY id", (now,)
                ).fetchall()
            ]
        return {"released_batch_ids": released}

    # ================= 内部实现 =================
    def _checkpoint(self, item_id: int, payload: dict[str, Any], *, forced: bool) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = SchedulingRepository(connection)
            item = self._require_item(connection, item_id)
            allowed = {"checkpointing", "checkpoint_failed"} if forced else {"checkpointing"}
            if item["state"] not in allowed:
                raise ConflictError(f"排空项状态 {item['state']} 不允许{'强制' if forced else ''}提交检查点")
            task = self._task(connection, item["task_id"])
            if task is None or task["status"] not in RUNNING_TASK_STATES:
                raise ConflictError("对应任务当前不在运行中，无法记录检查点")
            checkpoint = payload.get("checkpoint") or {}
            connection.execute(
                "UPDATE compute_tasks SET status='held',lease_owner='',lease_expires_at='',updated_at=?,version=version+1 WHERE id=?",
                (now, task["id"]),
            )
            repository.update_item(
                item_id, now, state="drained",
                checkpoint_json=json.dumps(checkpoint, ensure_ascii=False, sort_keys=True),
                checkpoint_at=now, updated_by=payload["actor"],
                exception_note=("forced:" + payload["reason"][:900]) if forced else "",
            )
            repository.add_audit(
                window_id=item["window_id"],
                action="item.checkpoint_forced" if forced else "item.checkpoint",
                actor=payload["actor"], outcome="success",
                detail={"item_id": item_id, "task_id": item["task_id"], "reason": payload.get("reason", ""), "checkpoint_keys": sorted(checkpoint)},
                now=now,
            )
            return self._serialize_item(connection, repository.item_by_id(item_id))

    def _snapshot_drain(self, connection: sqlite3.Connection, window: sqlite3.Row, now: str) -> None:
        repository = SchedulingRepository(connection)
        payloads = set(json.loads(window["payloads_json"]))
        tasks = connection.execute(
            "SELECT * FROM compute_tasks WHERE project_code IN (%s) ORDER BY id" % ",".join("?" for _ in payloads),
            tuple(payloads),
        ).fetchall() if payloads else []
        strategy = window["drain_strategy"]
        for task in tasks:
            if repository.item(window["id"], task["id"]) is not None:
                continue
            status = task["status"]
            if status == "queued":
                item_id = repository.add_item(window_id=window["id"], task_id=task["id"], item_role="queued", state="drained", task=task, now=now)
                connection.execute(
                    "UPDATE compute_tasks SET status='held',drain_window_id=?,updated_at=?,version=version+1 WHERE id=?",
                    (window["id"], now, task["id"]),
                )
                repository.update_item(item_id, now)
            elif status in RUNNING_TASK_STATES:
                if strategy == "pause_new":
                    item_id = repository.add_item(window_id=window["id"], task_id=task["id"], item_role="running", state="skipped", task=task, now=now)
                    repository.update_item(item_id, now, skip_reason="strategy_pause_new_keeps_running")
                else:
                    item_id = repository.add_item(window_id=window["id"], task_id=task["id"], item_role="running", state="checkpointing", task=task, now=now)
                connection.execute(
                    "UPDATE compute_tasks SET drain_window_id=?,updated_at=?,version=version+1 WHERE id=?",
                    (window["id"], now, task["id"]),
                )
            else:
                item_id = repository.add_item(window_id=window["id"], task_id=task["id"], item_role="succeeded", state="skipped", task=task, now=now)
                repository.update_item(item_id, now, skip_reason=f"already_{status}")

    def _build_restore_batches(self, connection: sqlite3.Connection, window: sqlite3.Row, now: str, *, base: str | None = None) -> None:
        repository = SchedulingRepository(connection)
        if repository.list_batches(window["id"]):
            return
        plan = json.loads(window["plan_json"] or "{}")
        size = int(plan.get("restore_batch_size", 50))
        interval = int(plan.get("restore_interval_seconds", 0))
        items = repository.items_in_states(window["id"], ["drained"])
        ordered = sorted(
            items,
            key=lambda item: (-int(item["original_priority"]), item["original_available_at"], item["task_id"]),
        )
        first_release = max(base or window["ends_at"], now)
        seq = 0
        for index in range(0, len(ordered), size):
            chunk = ordered[index:index + size]
            batch_no = index // size
            release_at = self._shift(first_release, batch_no * interval)
            batch_id = repository.add_batch(window_id=window["id"], batch_no=batch_no, release_at=release_at, now=now)
            for item in chunk:
                seq += 1
                repository.update_item(item["id"], now, restore_batch_id=batch_id, restore_seq=seq)

    def _release_due_batches(self, connection: sqlite3.Connection, window: sqlite3.Row, now: str) -> None:
        repository = SchedulingRepository(connection)
        for batch in repository.due_batches(now):
            if batch["window_id"] != window["id"]:
                continue
            self._release_batch(connection, batch, now)

    def _release_batch(self, connection: sqlite3.Connection, batch: sqlite3.Row, now: str) -> None:
        repository = SchedulingRepository(connection)
        items = connection.execute(
            "SELECT * FROM maintenance_window_items WHERE restore_batch_id=? ORDER BY restore_seq", (batch["id"],)
        ).fetchall()
        for item in items:
            if item["state"] == "restored":
                continue
            if item["state"] != "drained":
                continue
            self._restore_one(connection, item, now)
            repository.update_item(item["id"], now, state="restored", updated_by="scheduler")
        repository.mark_batch_released(batch["id"], now)

    def _restore_one(self, connection: sqlite3.Connection, item: sqlite3.Row, now: str) -> None:
        task = self._task(connection, item["task_id"])
        if task is None:
            raise ConflictError("对应计算任务已不存在")
        if task["status"] != "held":
            # 任务已被其他流程改变，按当前事实放行窗口标记
            connection.execute(
                "UPDATE compute_tasks SET drain_window_id=0,updated_at=?,version=version+1 WHERE id=?", (now, task["id"])
            )
            return
        connection.execute(
            "UPDATE compute_tasks SET status='queued',available_at=?,priority=?,drain_window_id=0,"
            "updated_at=?,version=version+1 WHERE id=?",
            (item["original_available_at"], item["original_priority"], now, task["id"]),
        )

    def _release_all_held(self, connection: sqlite3.Connection, window: sqlite3.Row, now: str, *, skip_reason: str) -> int:
        count = 0
        repository = SchedulingRepository(connection)
        for item in repository.list_items(window["id"]):
            if item["state"] == "restored":
                continue
            task = self._task(connection, item["task_id"])
            if item["state"] == "drained":
                self._restore_one(connection, item, now)
                count += 1
            elif task is not None and task["drain_window_id"] == window["id"]:
                # 仍在运行或异常的任务：解除窗口绑定，保持其当前状态
                connection.execute(
                    "UPDATE compute_tasks SET drain_window_id=0,updated_at=?,version=version+1 WHERE id=?", (now, task["id"])
                )
            if item["state"] not in {"skipped", "restored"}:
                repository.update_item(item["id"], now, state="skipped", skip_reason=skip_reason)
        return count

    def _detach_task(self, connection: sqlite3.Connection, item: sqlite3.Row, *, reason: str, now: str) -> None:
        task = self._task(connection, item["task_id"])
        if task is not None and task["drain_window_id"] == item["window_id"]:
            if task["status"] == "held":
                # 跳过即按原承诺放回队列，但不再纳入恢复批次
                connection.execute(
                    "UPDATE compute_tasks SET status='queued',available_at=?,priority=?,drain_window_id=0,"
                    "updated_at=?,version=version+1 WHERE id=?",
                    (item["original_available_at"], item["original_priority"], now, task["id"]),
                )
            else:
                connection.execute(
                    "UPDATE compute_tasks SET drain_window_id=0,updated_at=?,version=version+1 WHERE id=?", (now, task["id"])
                )

    def _reconcile(self, connection: sqlite3.Connection, window: sqlite3.Row) -> None:
        now_value = self.clock.now()
        now = to_storage(now_value)
        repository = SchedulingRepository(connection)
        if window["status"] in ACTIVE_STATES:
            self._reconcile_checkpointing(connection, window, now)
            window = repository.window_by_id(window["id"])
            if window["status"] in ACTIVE_STATES and now_value >= from_storage(window["ends_at"]):
                blocked = repository.items_in_states(window["id"], ["pending", "checkpointing", "checkpoint_failed"])
                if not blocked:
                    self._build_restore_batches(connection, window, now)
                    repository.update_window_status(window["id"], "recovering", now)
                    repository.add_audit(window_id=window["id"], action="restore.auto_begin", actor="scheduler", outcome="success", detail={}, now=now)
                    window = repository.window_by_id(window["id"])
        if window["status"] == "recovering":
            self._reconcile_checkpointing(connection, window, now)
            self._release_due_batches(connection, window, now)
            self._maybe_complete(connection, repository.window_by_id(window["id"]), now, "scheduler")

    def _reconcile_checkpointing(self, connection: sqlite3.Connection, window: sqlite3.Row, now: str) -> None:
        repository = SchedulingRepository(connection)
        for item in repository.items_in_states(window["id"], ["checkpointing", "checkpoint_failed"]):
            task = self._task(connection, item["task_id"])
            if task is None:
                repository.update_item(item["id"], now, state="skipped", skip_reason="task_missing")
                continue
            if task["status"] in TERMINAL_TASK_STATES:
                repository.update_item(item["id"], now, state="skipped", skip_reason=f"completed_naturally:{task['status']}")
                connection.execute("UPDATE compute_tasks SET drain_window_id=0 WHERE id=?", (task["id"],))
            elif task["status"] == "queued" and item["state"] == "checkpointing":
                # 租约恢复把任务放回了队列：窗口内继续挂起
                connection.execute(
                    "UPDATE compute_tasks SET status='held',lease_owner='',lease_expires_at='',updated_at=?,version=version+1 WHERE id=?",
                    (now, task["id"]),
                )
                repository.update_item(item["id"], now, state="drained", checkpoint_at=now, exception_note="lease_recovered_then_held")

    def _maybe_complete(self, connection: sqlite3.Connection, window: sqlite3.Row | None, now: str, actor: str) -> None:
        if window is None or window["status"] != "recovering":
            return
        repository = SchedulingRepository(connection)
        pending_batches = connection.execute(
            "SELECT COUNT(*) FROM maintenance_restore_batches WHERE window_id=? AND status='pending'", (window["id"],)
        ).fetchone()[0]
        open_items = repository.items_in_states(window["id"], ["pending", "checkpointing", "checkpoint_failed", "restoring", "drained"])
        if pending_batches == 0 and not open_items:
            repository.update_window_status(window["id"], "completed", now, completed_at=now)
            repository.add_audit(window_id=window["id"], action="window.complete", actor=actor, outcome="success", detail={}, now=now)

    def _build_plan(self, connection: sqlite3.Connection, payload: dict[str, Any], now: str) -> dict[str, Any]:
        payloads = sorted(set(payload["payloads"]))
        if payloads:
            placeholders = ",".join("?" for _ in payloads)
            rows = connection.execute(
                f"SELECT id,status,priority FROM compute_tasks WHERE project_code IN ({placeholders}) ORDER BY priority DESC,created_at ASC,id ASC",
                tuple(payloads),
            ).fetchall()
        else:
            rows = []
        counts: dict[str, int] = {}
        task_ids: list[int] = []
        for row in rows:
            counts[row["status"]] = counts.get(row["status"], 0) + 1
            task_ids.append(row["id"])
        held = counts.get("queued", 0) + (
            counts.get("running", 0) + counts.get("cancel_requested", 0)
            if payload["drain_strategy"] != "pause_new" else 0
        )
        batch_size = int(payload.get("restore_batch_size", 50))
        return {
            "generated_at": now,
            "payloads": payloads,
            "task_ids": task_ids,
            "task_counts": counts,
            "will_hold": held,
            "restore_batch_size": batch_size,
            "restore_interval_seconds": int(payload.get("restore_interval_seconds", 0)),
            "restore_batches": (held + batch_size - 1) // batch_size if batch_size else 0,
        }

    # ---------- 序列化与辅助 ----------
    def _window_detail(self, connection: sqlite3.Connection, window: sqlite3.Row) -> dict[str, Any]:
        repository = SchedulingRepository(connection)
        result = self._serialize_window(window)
        items = repository.list_items(window["id"])
        serialized = [self._serialize_item(connection, item) for item in items]
        result["items"] = serialized
        result["batches"] = repository.list_batches(window["id"])
        counts: dict[str, int] = {}
        for item in items:
            counts[item["state"]] = counts.get(item["state"], 0) + 1
        result["progress"] = {
            "total": len(items),
            "by_state": counts,
            "drained": counts.get("drained", 0),
            "restored": counts.get("restored", 0),
            "skipped": counts.get("skipped", 0),
            "awaiting_checkpoint": counts.get("checkpointing", 0) + counts.get("checkpoint_failed", 0),
            "failed": counts.get("failed", 0),
        }
        result["restore_order"] = [
            {"seq": item["restore_seq"], "item_id": item["id"], "task_id": item["task_id"],
             "batch_id": item["restore_batch_id"], "state": item["state"]}
            for item in sorted(serialized, key=lambda value: (value["restore_seq"] is None, value["restore_seq"] or 0))
            if item["restore_seq"] is not None
        ]
        result["skip_reasons"] = sorted({
            item["skip_reason"] for item in serialized if item["skip_reason"]
        })
        return result

    @staticmethod
    def _serialize_window(window: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        result = dict(window)
        result["payloads"] = json.loads(result.pop("payloads_json") or "[]")
        result["plan"] = json.loads(result.pop("plan_json") or "{}")
        return result

    def _serialize_item(self, connection: sqlite3.Connection, item: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        result = dict(item)
        result["checkpoint"] = json.loads(result.pop("checkpoint_json") or "{}")
        task = connection.execute(
            "SELECT id,status,priority,project_code,lease_owner,drain_window_id FROM compute_tasks WHERE id=?",
            (result["task_id"],),
        ).fetchone()
        result["task"] = dict(task) if task else None
        return result

    @staticmethod
    def _task(connection: sqlite3.Connection, task_id: int) -> sqlite3.Row | None:
        return connection.execute("SELECT * FROM compute_tasks WHERE id=?", (task_id,)).fetchone()

    def _require_window(self, connection: sqlite3.Connection, code: str) -> sqlite3.Row:
        window = SchedulingRepository(connection).window_by_code(code)
        if window is None:
            raise NotFoundError("维护窗口不存在")
        return window

    def _require_item(self, connection: sqlite3.Connection, item_id: int) -> sqlite3.Row:
        item = SchedulingRepository(connection).item_by_id(item_id)
        if item is None:
            raise NotFoundError("排空项不存在")
        return item

    @staticmethod
    def _parse_time(value: str, field: str) -> Any:
        try:
            parsed = from_storage(value)
        except ValueError as exc:
            raise ValidationError(f"{field} 不是合法的 ISO 8601 时间") from exc
        if parsed is None:
            raise ValidationError(f"{field} 不能为空")
        return parsed

    @staticmethod
    def _shift(value: str, seconds: int) -> str:
        if not seconds:
            return value
        return to_storage(from_storage(value) + timedelta(seconds=seconds))
