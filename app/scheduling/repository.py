from __future__ import annotations

import json
import sqlite3
from typing import Any, Iterable


class SchedulingRepository:
    """维护排程窗口、排空项、恢复批次与审计事件的 SQLite 读写。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    # ---- 窗口 ----
    def window_by_code(self, code: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM maintenance_windows WHERE code=?", (code,)).fetchone()

    def window_by_id(self, window_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM maintenance_windows WHERE id=?", (window_id,)).fetchone()

    def list_windows(self, status: str | None, limit: int) -> list[dict[str, Any]]:
        if status:
            rows = self.connection.execute(
                "SELECT * FROM maintenance_windows WHERE status=? ORDER BY id DESC LIMIT ?", (status, limit)
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM maintenance_windows ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(row) for row in rows]

    def create_window(self, *, payload: dict[str, Any], plan: dict[str, Any], now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO maintenance_windows(code,title,window_type,payloads_json,drain_strategy,"
            "starts_at,ends_at,planned_ends_at,grace_period_seconds,plan_json,created_by,created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                payload["code"], payload["title"], payload["window_type"],
                json.dumps(payload["payloads"], ensure_ascii=False), payload["drain_strategy"],
                payload["starts_at"], payload["ends_at"], payload["ends_at"],
                payload["grace_period_seconds"], json.dumps(plan, ensure_ascii=False),
                payload["created_by"], now, now,
            ),
        )
        return dict(self.window_by_id(cursor.lastrowid))

    def update_window_status(
        self, window_id: int, status: str, now: str, **fields: Any
    ) -> None:
        assignments = ["status=?", "updated_at=?"]
        values: list[Any] = [status, now]
        for key, value in fields.items():
            assignments.append(f"{key}=?")
            values.append(value)
        values.append(window_id)
        self.connection.execute(
            f"UPDATE maintenance_windows SET {','.join(assignments)} WHERE id=?", values
        )

    # ---- 排空项 ----
    def item_by_id(self, item_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM maintenance_window_items WHERE id=?", (item_id,)).fetchone()

    def item(self, window_id: int, task_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM maintenance_window_items WHERE window_id=? AND task_id=?", (window_id, task_id)
        ).fetchone()

    def list_items(self, window_id: int, state: str | None = None) -> list[dict[str, Any]]:
        if state:
            rows = self.connection.execute(
                "SELECT * FROM maintenance_window_items WHERE window_id=? AND state=? ORDER BY id",
                (window_id, state),
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM maintenance_window_items WHERE window_id=? ORDER BY id", (window_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def items_in_states(self, window_id: int, states: Iterable[str]) -> list[sqlite3.Row]:
        placeholders = ",".join("?" for _ in states)
        return self.connection.execute(
            f"SELECT * FROM maintenance_window_items WHERE window_id=? AND state IN ({placeholders}) ORDER BY id",
            (window_id, *states),
        ).fetchall()

    def add_item(self, *, window_id: int, task_id: int, item_role: str, state: str, task: sqlite3.Row, now: str) -> int:
        cursor = self.connection.execute(
            "INSERT INTO maintenance_window_items(window_id,task_id,item_role,state,original_status,"
            "original_available_at,original_priority,original_lease_owner,original_lease_expires_at,"
            "original_attempt_count,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                window_id, task_id, item_role, state, task["status"], task["available_at"], task["priority"],
                task["lease_owner"], task["lease_expires_at"], task["attempt_count"], now, now,
            ),
        )
        return int(cursor.lastrowid)

    def update_item(self, item_id: int, now: str, **fields: Any) -> None:
        assignments = ["updated_at=?"]
        values: list[Any] = [now]
        for key, value in fields.items():
            assignments.append(f"{key}=?")
            values.append(value)
        values.append(item_id)
        self.connection.execute(
            f"UPDATE maintenance_window_items SET {','.join(assignments)} WHERE id=?", values
        )

    # ---- 恢复批次 ----
    def add_batch(self, *, window_id: int, batch_no: int, release_at: str, now: str) -> int:
        cursor = self.connection.execute(
            "INSERT INTO maintenance_restore_batches(window_id,batch_no,release_at,created_at,updated_at) "
            "VALUES(?,?,?,?,?)",
            (window_id, batch_no, release_at, now, now),
        )
        return int(cursor.lastrowid)

    def list_batches(self, window_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM maintenance_restore_batches WHERE window_id=? ORDER BY batch_no", (window_id,)
        ).fetchall()
        return [dict(row) for row in rows]

    def batch_by_id(self, batch_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM maintenance_restore_batches WHERE id=?", (batch_id,)
        ).fetchone()

    def due_batches(self, now: str) -> list[sqlite3.Row]:
        return self.connection.execute(
            "SELECT * FROM maintenance_restore_batches WHERE status='pending' AND release_at<=? ORDER BY release_at,id",
            (now,),
        ).fetchall()

    def mark_batch_released(self, batch_id: int, now: str) -> None:
        self.connection.execute(
            "UPDATE maintenance_restore_batches SET status='released',released_at=?,updated_at=? WHERE id=?",
            (now, now, batch_id),
        )

    # ---- 审计 ----
    def add_audit(self, *, window_id: int | None, action: str, actor: str, outcome: str, detail: dict[str, Any], now: str) -> int:
        cursor = self.connection.execute(
            "INSERT INTO maintenance_audit_events(window_id,action,actor,outcome,detail_json,created_at) VALUES(?,?,?,?,?,?)",
            (window_id, action, actor, outcome, json.dumps(detail, ensure_ascii=False, sort_keys=True), now),
        )
        return int(cursor.lastrowid)

    def list_audit(self, window_id: int | None, limit: int) -> list[dict[str, Any]]:
        if window_id is None:
            rows = self.connection.execute(
                "SELECT * FROM maintenance_audit_events ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM maintenance_audit_events WHERE window_id=? ORDER BY id DESC LIMIT ?",
                (window_id, limit),
            ).fetchall()
        return [dict(row) for row in rows]
