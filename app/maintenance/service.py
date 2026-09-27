from __future__ import annotations

import json
import sqlite3
from datetime import UTC, timedelta
from typing import Any

from app.core.clock import Clock, SystemClock, from_storage, to_storage
from app.core.errors import ConflictError, NotFoundError
from app.database import get_connection, transaction
from app.maintenance.repository import TERMINAL_CATEGORIES, MaintenanceRepository


class MaintenanceScheduleService:
    """维护窗口的预演、启用、排空、检查点、恢复批次与异常处置。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = MaintenanceRepository(self.connection)

    # ---- 窗口创建与查询 ----

    def create_window(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        values = {
            **payload,
            "payloads": sorted(set(payload["payloads"])),
            "projects": sorted(set(payload["projects"])),
            "planned_start_at": to_storage(payload["planned_start_at"]),
            "planned_end_at": to_storage(payload["planned_end_at"]),
        }
        with transaction(immediate=True) as connection:
            repository = MaintenanceRepository(connection)
            if repository.window_by_code(values["code"]):
                raise ConflictError("维护窗口编码已存在")
            window = repository.create_window(values=values, now=now)
            repository.add_event(window_id=window["id"], task_id=None, event_type="window_created",
                                 actor=values["created_by"], detail={"code": values["code"]}, now=now)
            return self._serialize(window)

    def list_windows(self, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return [self._serialize(row) for row in self.repository.list_windows(status=status, limit=max(1, min(limit, 500)))]

    def get_window(self, code: str) -> dict[str, Any]:
        return self._serialize(self._require_window_row(code))

    # ---- 预演（只读，不落库）----

    def preview(self, code: str) -> dict[str, Any]:
        window = self._require_window_row(code)
        repository = self.repository
        tasks = repository.affected_tasks(json.loads(window["payloads_json"]), json.loads(window["projects_json"]))
        affected: list[dict[str, Any]] = []
        counts: dict[str, int] = {}
        for task in tasks:
            plan = self._drain_plan_for_status(task["status"], window["drain_strategy"])
            counts[plan] = counts.get(plan, 0) + 1
            affected.append({
                "task_id": task["id"], "project_code": task["project_code"], "payload_code": task["payload_code"],
                "status": task["status"], "priority": task["priority"], "queue_ticket": task["queue_ticket"],
                "drain_plan": plan,
            })
        shelvable = [item for item in affected if item["drain_plan"] in {"shelve", "checkpoint_then_shelve", "cancel_signal_then_checkpoint"}]
        ordered = self._order_for_restore(window, shelvable)
        size = int(window["restore_batch_size"])
        batches = [len(chunk) for chunk in (ordered[i:i + size] for i in range(0, len(ordered), size))]
        return {
            "window": self._serialize(window),
            "affected_count": len(affected),
            "counts": counts,
            "affected": affected,
            "rollback_terminal": False,
            "projected_restore_batches": batches,
            "projected_restore_order": [item["task_id"] for item in ordered],
        }

    @staticmethod
    def _drain_plan_for_status(status: str, strategy: str) -> str:
        if status == "queued":
            return "leave_queued_frozen" if strategy == "checkpoint_only" else "shelve"
        if status in {"running", "cancel_requested"}:
            return "cancel_signal_then_checkpoint" if strategy == "immediate" and status == "running" else "checkpoint_then_shelve"
        return "keep_terminal"

    # ---- 启用（幂等）----

    def activate(self, code: str, actor: str, force: bool = False) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = MaintenanceRepository(connection)
            window = self._locked_window(repository, code)
            if window["status"] == "cancelled":
                raise ConflictError("维护窗口已取消，不能启用")
            if window["status"] != "preview":
                # 重复启用：原样返回，不重复排空
                return self._detail(repository, window)
            if now_value < from_storage(window["planned_start_at"]) and not force:
                raise ConflictError("尚未到达计划开始时间，提前启用需要 force=true")
            repository.touch_window(window["id"], now, status="active", actual_start_at=now, activated_at=now)
            self._snapshot_affected(connection, repository, window, now)
            repository.add_event(window_id=window["id"], task_id=None, event_type="window_activated",
                                 actor=actor, detail={"force": force}, now=now)
            window = repository.window_by_id(window["id"])
            return self._detail(repository, window)

    def _snapshot_affected(self, connection: sqlite3.Connection, repository: MaintenanceRepository, window: sqlite3.Row, now: str) -> int:
        tasks = repository.affected_tasks(json.loads(window["payloads_json"]), json.loads(window["projects_json"]))
        tracked = 0
        for task in tasks:
            if self._classify_and_drain(connection, repository, window, task, now, category_override=None):
                tracked += 1
        return tracked

    def _classify_and_drain(self, connection: sqlite3.Connection, repository: MaintenanceRepository, window: sqlite3.Row, task: sqlite3.Row, now: str, *, category_override: str | None) -> bool:
        """返回 True 表示该任务已纳入窗口条目。"""
        status = task["status"]
        category = category_override or status
        strategy = window["drain_strategy"]
        if status == "queued":
            if strategy == "checkpoint_only":
                # checkpoint_only：排队任务不纳入窗口，仅靠领取冻结避免窗口期间执行
                return False
            connection.execute(
                "UPDATE compute_tasks SET status='shelved',updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                (now, task["id"]),
            )
            repository.insert_item(window_id=window["id"], task=task, category=category, drain_state="shelved", restore_state="pending", now=now)
            return True
        if status in {"running", "cancel_requested"}:
            if strategy == "immediate" and status == "running":
                # 立即排空：向持单工作者发出协作式取消信号，催促其尽快提交检查点
                connection.execute(
                    "UPDATE compute_tasks SET status='cancel_requested',updated_at=?,version=version+1 WHERE id=? AND status='running'",
                    (now, task["id"]),
                )
            repository.insert_item(window_id=window["id"], task=task, category=category, drain_state="checkpoint_pending", restore_state="pending", now=now)
            return True
        if status in TERMINAL_CATEGORIES:
            drain_state = {"succeeded": "completed", "failed": "failed_terminal", "cancelled": "cancelled_terminal"}[status]
            repository.insert_item(window_id=window["id"], task=task, category=category, drain_state=drain_state, restore_state="not_required", now=now)
            return True
        if status == "shelved":
            # 已被其他窗口搁置的任务：登记但不在本窗口重复恢复
            if repository.item_by_task(window["id"], task["id"]) is None:
                repository.insert_item(window_id=window["id"], task=task, category="queued", drain_state="skipped", restore_state="skipped", now=now)
                created = repository.item_by_task(window["id"], task["id"])
                repository.touch_item(created["id"], now, skip_reason="任务已被其他维护窗口搁置，本窗口不重复恢复")
            return True
        return False

    # ---- 对账：窗口期间新提交 / 租约恢复 / 运行中完成 ----

    def _reconcile(self, connection: sqlite3.Connection, repository: MaintenanceRepository, window: sqlite3.Row, now: str) -> None:
        if window["status"] not in {"active", "ending"}:
            return
        # 新出现的受影响任务（窗口期间提交）
        tasks = repository.affected_tasks(json.loads(window["payloads_json"]), json.loads(window["projects_json"]))
        for task in tasks:
            if repository.item_by_task(window["id"], task["id"]) is None:
                tracked = self._classify_and_drain(connection, repository, window, task, now, category_override="new")
                if tracked and task["status"] == "queued" and window["drain_strategy"] != "checkpoint_only":
                    repository.add_event(window_id=window["id"], task_id=task["id"], event_type="task_auto_shelved",
                                         actor="system", detail={"category": "new"}, now=now)
        # 已登记但任务状态发生变化的条目
        for item in repository.items_in_drain_states(window["id"], ["checkpoint_pending"]):
            task = connection.execute("SELECT * FROM compute_tasks WHERE id=?", (item["task_id"],)).fetchone()
            if task is None:
                continue
            if task["status"] == "succeeded":
                repository.touch_item(item["id"], now, drain_state="completed", restore_state="not_required")
                repository.add_event(window_id=window["id"], task_id=task["id"], event_type="task_finished_terminal",
                                     actor="system", detail={"status": "succeeded"}, now=now)
            elif task["status"] == "failed":
                repository.touch_item(item["id"], now, drain_state="failed_terminal", restore_state="not_required")
                repository.add_event(window_id=window["id"], task_id=task["id"], event_type="task_finished_terminal",
                                     actor="system", detail={"status": "failed"}, now=now)
            elif task["status"] == "cancelled":
                repository.touch_item(item["id"], now, drain_state="cancelled_terminal", restore_state="not_required")
                repository.add_event(window_id=window["id"], task_id=task["id"], event_type="task_finished_terminal",
                                     actor="system", detail={"status": "cancelled"}, now=now)
            elif task["status"] == "queued":
                # 租约过期恢复后重新排队：无检查点，直接搁置，恢复时从头重跑
                connection.execute(
                    "UPDATE compute_tasks SET status='shelved',updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                    (now, task["id"]),
                )
                repository.touch_item(item["id"], now, drain_state="shelved")
                repository.add_event(window_id=window["id"], task_id=task["id"], event_type="task_auto_shelved",
                                     actor="system", detail={"reason": "lease_recovered"}, now=now)

    # ---- 检查点 ----

    def submit_checkpoint(self, code: str, payload: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = MaintenanceRepository(connection)
            window = self._locked_window(repository, code)
            if window["status"] not in {"active", "ending"}:
                raise ConflictError("只有进行中的维护窗口可以接收检查点")
            self._reconcile(connection, repository, window, now)
            task = connection.execute("SELECT * FROM compute_tasks WHERE id=?", (payload["task_id"],)).fetchone()
            if task is None:
                raise NotFoundError("计算任务不存在")
            item = repository.item_by_task(window["id"], task["id"])
            if item is None:
                raise ConflictError("该任务不属于此维护窗口的受影响载荷")
            if item["drain_state"] != "checkpoint_pending":
                raise ConflictError("该任务不处于等待检查点状态", context={"drain_state": item["drain_state"]})
            if task["status"] not in {"running", "cancel_requested"} or task["lease_owner"] != payload["worker_id"]:
                raise ConflictError("只有持有任务租约的工作者可以提交检查点")
            checkpoint = repository.add_checkpoint(
                window_id=window["id"], task_id=task["id"], worker_id=payload["worker_id"],
                checkpoint_data=payload["checkpoint_data"], progress_percent=payload["progress_percent"],
                created_by=payload["worker_id"], now=now,
            )
            connection.execute(
                "UPDATE compute_tasks SET status='shelved',lease_owner='',lease_expires_at='',updated_at=?,version=version+1 WHERE id=?",
                (now, task["id"]),
            )
            repository.touch_item(item["id"], now, drain_state="shelved", restore_state="pending")
            repository.add_event(window_id=window["id"], task_id=task["id"], event_type="checkpoint_submitted",
                                 actor=payload["worker_id"], detail={"checkpoint_id": checkpoint["id"], "sequence": checkpoint["sequence"]}, now=now)
            window = repository.window_by_id(window["id"])
            return self._detail(repository, window)

    # ---- 排空进度 ----

    def progress(self, code: str) -> dict[str, Any]:
        with transaction(immediate=True) as connection:
            repository = MaintenanceRepository(connection)
            window = self._locked_window(repository, code)
            now = to_storage(self.clock.now())
            self._reconcile(connection, repository, window, now)
            return self._progress(repository, window)

    def _progress(self, repository: MaintenanceRepository, window: sqlite3.Row) -> dict[str, Any]:
        items = repository.list_items(window["id"])
        drain_counts: dict[str, int] = {}
        restore_counts: dict[str, int] = {}
        pending_checkpoints: list[dict[str, Any]] = []
        open_exceptions: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for item in items:
            drain_counts[item["drain_state"]] = drain_counts.get(item["drain_state"], 0) + 1
            restore_counts[item["restore_state"]] = restore_counts.get(item["restore_state"], 0) + 1
            if item["drain_state"] == "checkpoint_pending":
                pending_checkpoints.append(self._item_brief(item))
            if item["exception_status"] == "open":
                open_exceptions.append(self._item_brief(item))
            if item["restore_state"] == "skipped" or item["drain_state"] == "skipped":
                skipped.append(self._item_brief(item))
        effective_end = window["extended_end_at"] or window["planned_end_at"]
        return {
            "window": self._serialize(window),
            "total_items": len(items),
            "drain": drain_counts,
            "restore": restore_counts,
            "drain_complete": drain_counts.get("checkpoint_pending", 0) == 0,
            "pending_checkpoints": pending_checkpoints,
            "open_exceptions": open_exceptions,
            "skipped": skipped,
            "effective_end_at": effective_end,
        }

    # ---- 结束排空并生成恢复批次 ----

    def end_drain(self, code: str, actor: str, force: bool = False) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = MaintenanceRepository(connection)
            window = self._locked_window(repository, code)
            if window["status"] in {"ending", "restoring", "completed"}:
                return self._detail(repository, window)
            if window["status"] != "active":
                raise ConflictError("只有启用中的维护窗口可以结束排空", context={"status": window["status"]})
            self._reconcile(connection, repository, window, now)
            pending = repository.items_in_drain_states(window["id"], ["checkpoint_pending"])
            if pending and not force:
                raise ConflictError("仍有运行中任务未提交检查点", context={"task_ids": [item["task_id"] for item in pending]})
            for item in pending:
                repository.touch_item(item["id"], now, drain_state="skipped", restore_state="not_required",
                                      skip_reason="窗口结束时未收到检查点，强制结束排空")
                self._revoke_cancel_signal(connection, item)
                repository.add_event(window_id=window["id"], task_id=item["task_id"], event_type="item_skipped",
                                     actor=actor, detail={"phase": "drain", "reason": "missing_checkpoint_force"}, now=now)
            self._build_restore_batches(connection, repository, window, now)
            repository.touch_window(window["id"], now, status="restoring", actual_end_at=now, drain_completed_at=now)
            repository.add_event(window_id=window["id"], task_id=None, event_type="drain_ended",
                                 actor=actor, detail={"forced": bool(pending)}, now=now)
            # 没有需要恢复的任务时直接完成
            detail = self._progress(repository, repository.window_by_id(window["id"]))
            if detail["restore"].get("pending", 0) == 0 and detail["restore"].get("batched", 0) == 0:
                repository.touch_window(window["id"], now, status="completed")
                repository.add_event(window_id=window["id"], task_id=None, event_type="window_completed",
                                     actor=actor, detail={"automatic": True}, now=now)
            return self._detail(repository, repository.window_by_id(window["id"]))

    def _build_restore_batches(self, connection: sqlite3.Connection, repository: MaintenanceRepository, window: sqlite3.Row, now: str) -> None:
        items = [dict(item) for item in repository.list_items(window["id"])
                 if item["drain_state"] == "shelved" and item["restore_state"] == "pending"]
        ordered = self._order_for_restore(window, items)
        size = int(window["restore_batch_size"])
        interval = int(window["restore_interval_seconds"])
        end_at = from_storage(window["extended_end_at"] or window["planned_end_at"])
        for index, chunk in enumerate(ordered[i:i + size] for i in range(0, len(ordered), size)):
            batch_number = index + 1
            planned_at = to_storage(end_at + timedelta(seconds=interval * index))
            batch_id = repository.create_batch(window_id=window["id"], batch_number=batch_number, planned_at=planned_at, now=now)
            for item in chunk:
                repository.touch_item(item["id"], now, batch_number=batch_number, restore_state="batched")
            repository.touch_batch(batch_id, now, item_count=len(chunk))

    @staticmethod
    def _order_for_restore(window: sqlite3.Row, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        def priority_of(row: dict[str, Any]) -> int:
            return int(row.get("priority", row.get("task_priority", 0)))

        if window["restore_order"] == "priority":
            return sorted(rows, key=lambda row: (-priority_of(row), int(row["queue_ticket"]), int(row["task_id"])))
        return sorted(rows, key=lambda row: (int(row["queue_ticket"]), int(row["task_id"])))

    # ---- 恢复顺序与批次释放 ----

    def restore_plan(self, code: str) -> dict[str, Any]:
        window = self._require_window_row(code)
        repository = self.repository
        items = repository.list_items(window["id"])
        restorable = [item for item in items if item["drain_state"] == "shelved"]
        ordered = self._order_for_restore(window, restorable)
        batches = repository.list_batches(window["id"])
        for batch in batches:
            batch["items"] = [self._item_brief(item) for item in ordered if item.get("batch_number") == batch["batch_number"]]
        return {
            "window": self._serialize(window),
            "restore_order": window["restore_order"],
            "order": [self._item_brief(item) for item in ordered],
            "batches": batches,
        }

    def release_batch(self, code: str, actor: str, batch_number: int | None = None, only_due: bool = False) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = MaintenanceRepository(connection)
            window = self._locked_window(repository, code)
            if window["status"] not in {"restoring"}:
                if window["status"] == "completed":
                    return self._detail(repository, window)
                raise ConflictError("窗口尚未进入恢复阶段", context={"status": window["status"]})
            batches = repository.list_batches(window["id"])
            target = self._select_batch(window, batches, batch_number, only_due, now_value)
            if target is None:
                return self._detail(repository, window)
            if target["status"] == "released":
                # 重复释放幂等：直接返回现状
                return self._detail(repository, window)
            released_items = repository.list_items(window["id"])
            released = 0
            for item in released_items:
                if item["batch_number"] != target["batch_number"] or item["restore_state"] != "batched":
                    continue
                connection.execute(
                    "UPDATE compute_tasks SET status='queued',available_at=?,updated_at=?,version=version+1 WHERE id=? AND status='shelved'",
                    (now, now, item["task_id"]),
                )
                checkpoint = repository.latest_checkpoint(window["id"], item["task_id"])
                repository.touch_item(item["id"], now, restore_state="restored")
                repository.add_event(window_id=window["id"], task_id=item["task_id"], event_type="task_restored",
                                     actor=actor,
                                     detail={"batch_number": target["batch_number"],
                                             "from_checkpoint_id": checkpoint["id"] if checkpoint else None},
                                     now=now)
                released += 1
            repository.touch_batch(target["id"], now, status="released", released_at=now)
            repository.add_event(window_id=window["id"], task_id=None, event_type="restore_batch_released",
                                 actor=actor, detail={"batch_number": target["batch_number"], "released": released}, now=now)
            self._maybe_complete(repository, window, actor, now)
            return self._detail(repository, repository.window_by_id(window["id"]))

    @staticmethod
    def _select_batch(window: sqlite3.Row, batches: list[dict[str, Any]], requested: int | None, only_due: bool, now_value: Any) -> dict[str, Any] | None:
        del window
        planned = sorted(batches, key=lambda batch: batch["batch_number"])
        if only_due:
            for batch in planned:
                if batch["status"] == "planned" and batch["planned_at"] and from_storage(batch["planned_at"]) <= now_value:
                    return batch
            return None
        if requested is not None:
            for batch in planned:
                if batch["batch_number"] == requested:
                    return batch
            raise NotFoundError("恢复批次不存在")
        # 默认释放最早的未释放批次（运维显式推进）
        for batch in planned:
            if batch["status"] == "planned":
                return batch
        return None

    def _maybe_complete(self, repository: MaintenanceRepository, window: sqlite3.Row, actor: str, now: str) -> None:
        progress = self._progress(repository, window)
        if progress["restore"].get("batched", 0) == 0 and progress["restore"].get("pending", 0) == 0:
            current = repository.window_by_id(window["id"])
            if current["status"] == "restoring":
                repository.touch_window(window["id"], now, status="completed")
                repository.add_event(window_id=window["id"], task_id=None, event_type="window_completed",
                                     actor=actor, detail={"automatic": False}, now=now)

    # ---- 临时延长 ----

    def extend(self, code: str, payload: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = MaintenanceRepository(connection)
            window = self._locked_window(repository, code)
            if window["status"] not in {"active", "ending"}:
                raise ConflictError("只有进行中的维护窗口可以延长", context={"status": window["status"]})
            current_end = from_storage(window["extended_end_at"] or window["planned_end_at"])
            if payload.get("new_end_at") is not None:
                new_end_value = payload["new_end_at"]
            else:
                new_end_value = current_end + timedelta(seconds=int(payload["extend_seconds"]))
            if new_end_value.tzinfo is None:
                new_end_value = new_end_value.replace(tzinfo=UTC)
            new_end_storage = to_storage(new_end_value)
            if from_storage(new_end_storage) <= current_end:
                raise ConflictError("新的结束时间必须晚于当前有效结束时间")
            repository.touch_window(window["id"], now, extended_end_at=new_end_storage)
            repository.add_event(window_id=window["id"], task_id=None, event_type="window_extended",
                                 actor=payload["actor"], detail={"new_end_at": new_end_storage, "reason": payload["reason"]}, now=now)
            return self._serialize(repository.window_by_id(window["id"]))

    # ---- 取消 ----

    def cancel(self, code: str, actor: str, reason: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = MaintenanceRepository(connection)
            window = self._locked_window(repository, code)
            if window["status"] == "cancelled":
                return self._detail(repository, window)
            if window["status"] == "completed":
                raise ConflictError("已完成的维护窗口不能取消")
            if window["status"] == "preview":
                repository.touch_window(window["id"], now, status="cancelled", cancelled_at=now, cancel_reason=reason, actual_end_at=now)
                repository.add_event(window_id=window["id"], task_id=None, event_type="window_cancelled",
                                     actor=actor, detail={"reason": reason, "before_activation": True}, now=now)
                return self._detail(repository, repository.window_by_id(window["id"]))
            self._reconcile(connection, repository, window, now)
            # 运行中未交检查点的任务：窗口取消，保持运行不动，不参与恢复
            for item in repository.items_in_drain_states(window["id"], ["checkpoint_pending"]):
                repository.touch_item(item["id"], now, drain_state="skipped", restore_state="not_required",
                                      skip_reason=f"窗口取消：{reason}")
                self._revoke_cancel_signal(connection, item)
            # 已搁置任务全部按承诺顺序立即回到队列
            shelved = [item for item in repository.list_items(window["id"])
                       if item["drain_state"] == "shelved" and item["restore_state"] in {"pending", "batched"}]
            ordered = self._order_for_restore(window, shelved)
            for item in ordered:
                connection.execute(
                    "UPDATE compute_tasks SET status='queued',available_at=?,updated_at=?,version=version+1 WHERE id=? AND status='shelved'",
                    (now, now, item["task_id"]),
                )
                fields: dict[str, Any] = {"restore_state": "restored"}
                if item["restore_state"] == "pending":
                    fields["batch_number"] = None
                repository.touch_item(item["id"], now, **fields)
                repository.add_event(window_id=window["id"], task_id=item["task_id"], event_type="task_restored",
                                     actor=actor, detail={"cancel_restore": True}, now=now)
            for batch in repository.list_batches(window["id"]):
                if batch["status"] != "released":
                    repository.touch_batch(batch["id"], now, status="released", released_at=now)
            repository.touch_window(window["id"], now, status="cancelled", cancelled_at=now,
                                    cancel_reason=reason, actual_end_at=now, drain_completed_at=now)
            repository.add_event(window_id=window["id"], task_id=None, event_type="window_cancelled",
                                 actor=actor, detail={"reason": reason, "restored": len(ordered)}, now=now)
            return self._detail(repository, repository.window_by_id(window["id"]))

    def _revoke_cancel_signal(self, connection: sqlite3.Connection, item: sqlite3.Row) -> None:
        """取消/跳过时，若 cancel_requested 是排空时由 running 置位的，则恢复为 running。"""
        try:
            snapshot = json.loads(item["snapshot_json"] or "{}")
        except json.JSONDecodeError:
            snapshot = {}
        if snapshot.get("status") != "running":
            return
        connection.execute(
            "UPDATE compute_tasks SET status='running',updated_at=updated_at WHERE id=? AND status='cancel_requested'",
            (item["task_id"],),
        )

    # ---- 逐项异常处理 ----

    def skip_item(self, code: str, task_id: int, actor: str, reason: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = MaintenanceRepository(connection)
            window = self._locked_window(repository, code)
            item = self._require_item(repository, window, task_id)
            if item["drain_state"] == "checkpoint_pending":
                # 排空阶段跳过：任务保持运行，不再要求检查点，也不纳入恢复
                repository.touch_item(item["id"], now, drain_state="skipped", restore_state="not_required", skip_reason=reason)
                self._revoke_cancel_signal(connection, item)
            elif item["drain_state"] == "shelved" and item["restore_state"] in {"pending", "batched"}:
                repository.touch_item(item["id"], now, restore_state="skipped", skip_reason=reason)
            else:
                raise ConflictError("当前条目状态不允许跳过", context={"drain_state": item["drain_state"], "restore_state": item["restore_state"]})
            repository.add_event(window_id=window["id"], task_id=task_id, event_type="item_skipped",
                                 actor=actor, detail={"reason": reason}, now=now)
            if window["status"] == "restoring":
                self._maybe_complete(repository, window, actor, now)
            return self._detail(repository, repository.window_by_id(window["id"]))

    def restore_item_now(self, code: str, task_id: int, actor: str, reason: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = MaintenanceRepository(connection)
            window = self._locked_window(repository, code)
            item = self._require_item(repository, window, task_id)
            if item["drain_state"] != "shelved" or item["restore_state"] not in {"pending", "batched", "skipped"}:
                raise ConflictError("只有已搁置条目可以提前恢复",
                                    context={"drain_state": item["drain_state"], "restore_state": item["restore_state"]})
            task = connection.execute("SELECT * FROM compute_tasks WHERE id=?", (task_id,)).fetchone()
            if task is None or task["status"] != "shelved":
                raise ConflictError("任务当前不是搁置状态")
            checkpoint = repository.latest_checkpoint(window["id"], task_id)
            connection.execute(
                "UPDATE compute_tasks SET status='queued',available_at=?,updated_at=?,version=version+1 WHERE id=?",
                (now, now, task_id),
            )
            repository.touch_item(item["id"], now, restore_state="restored", skip_reason="")
            repository.add_event(window_id=window["id"], task_id=task_id, event_type="task_restored",
                                 actor=actor, detail={"manual": True, "reason": reason,
                                                      "from_checkpoint_id": checkpoint["id"] if checkpoint else None}, now=now)
            if window["status"] == "restoring":
                self._maybe_complete(repository, window, actor, now)
            return self._detail(repository, repository.window_by_id(window["id"]))

    def report_exception(self, code: str, task_id: int, actor: str, note: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = MaintenanceRepository(connection)
            window = self._locked_window(repository, code)
            item = self._require_item(repository, window, task_id)
            repository.touch_item(item["id"], now, exception_status="open", exception_note=note)
            repository.add_event(window_id=window["id"], task_id=task_id, event_type="exception_reported",
                                 actor=actor, detail={"note": note}, now=now)
            return self._detail(repository, repository.window_by_id(window["id"]))

    def resolve_exception(self, code: str, task_id: int, actor: str, note: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = MaintenanceRepository(connection)
            window = self._locked_window(repository, code)
            item = self._require_item(repository, window, task_id)
            if item["exception_status"] != "open":
                raise ConflictError("该条目没有待处理的异常")
            merged_note = f"{item['exception_note']} | 处理：{note}" if note else item["exception_note"]
            repository.touch_item(item["id"], now, exception_status="resolved", exception_note=merged_note[:2000])
            repository.add_event(window_id=window["id"], task_id=task_id, event_type="exception_resolved",
                                 actor=actor, detail={"note": note}, now=now)
            return self._detail(repository, repository.window_by_id(window["id"]))

    # ---- 审计事件 ----

    def list_events(self, code: str, *, event_type: str | None = None, task_id: int | None = None, limit: int = 200) -> dict[str, Any]:
        window = self._require_window_row(code)
        events = self.repository.list_events(window["id"], event_type=event_type, task_id=task_id, limit=limit)
        return {"window_code": code, "items": events}

    # ---- 内部工具 ----

    def _require_window_row(self, code: str) -> sqlite3.Row:
        window = self.repository.window_by_code(code)
        if window is None:
            raise NotFoundError("维护窗口不存在")
        return window

    def _locked_window(self, repository: MaintenanceRepository, code: str) -> sqlite3.Row:
        window = repository.window_by_code(code)
        if window is None:
            raise NotFoundError("维护窗口不存在")
        return window

    def _require_item(self, repository: MaintenanceRepository, window: sqlite3.Row, task_id: int) -> sqlite3.Row:
        item = repository.item_by_task(window["id"], task_id)
        if item is None:
            raise NotFoundError("维护窗口条目不存在")
        return item

    def _detail(self, repository: MaintenanceRepository, window: sqlite3.Row) -> dict[str, Any]:
        result = self._progress(repository, window)
        result["batches"] = repository.list_batches(window["id"])
        result["items"] = [self._item_brief(item) for item in repository.list_items(window["id"])]
        return result

    @staticmethod
    def _item_brief(item: dict[str, Any] | sqlite3.Row) -> dict[str, Any]:
        data = dict(item)
        snapshot = data.get("snapshot_json")
        if isinstance(snapshot, str):
            try:
                data["snapshot"] = json.loads(snapshot)
            except json.JSONDecodeError:
                data["snapshot"] = {}
        data.pop("snapshot_json", None)
        return data

    @staticmethod
    def _serialize(window: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        data = dict(window)
        data["payloads"] = json.loads(data.pop("payloads_json"))
        data["projects"] = json.loads(data.pop("projects_json"))
        return data
