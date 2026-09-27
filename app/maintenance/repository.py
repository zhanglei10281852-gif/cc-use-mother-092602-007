from __future__ import annotations

import json
import sqlite3
from typing import Any, Iterable

TERMINAL_CATEGORIES = {"succeeded", "failed", "cancelled"}


class MaintenanceRepository:
    """维护窗口、排空条目、检查点、恢复批次与审计事件的 SQLite 读写。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    # ---- 窗口 ----

    def create_window(self, *, values: dict[str, Any], now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            """
            INSERT INTO maintenance_windows(
                code,title,window_type,payloads_json,projects_json,drain_strategy,
                restore_batch_size,restore_interval_seconds,restore_order,
                planned_start_at,planned_end_at,status,created_by,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,'preview',?,?,?)
            """,
            (
                values["code"], values["title"], values["window_type"],
                json.dumps(values["payloads"], ensure_ascii=False, sort_keys=True),
                json.dumps(values["projects"], ensure_ascii=False, sort_keys=True),
                values["drain_strategy"], values["restore_batch_size"],
                values["restore_interval_seconds"], values["restore_order"],
                values["planned_start_at"], values["planned_end_at"],
                values["created_by"], now, now,
            ),
        )
        return dict(self.window_by_id(cursor.lastrowid))  # type: ignore[arg-type]

    def window_by_id(self, window_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM maintenance_windows WHERE id=?", (window_id,)).fetchone()

    def window_by_code(self, code: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM maintenance_windows WHERE code=?", (code,)).fetchone()

    def list_windows(self, *, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if status:
            rows = self.connection.execute(
                "SELECT * FROM maintenance_windows WHERE status=? ORDER BY id DESC LIMIT ?", (status, limit),
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM maintenance_windows ORDER BY id DESC LIMIT ?", (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def touch_window(self, window_id: int, now: str, **fields: Any) -> None:
        if not fields:
            return
        assignments = ", ".join(f"{key}=?" for key in fields)
        params = [*fields.values(), now, window_id]
        self.connection.execute(
            f"UPDATE maintenance_windows SET {assignments},updated_at=?,version=version+1 WHERE id=?",
            params,
        )

    # ---- 受影响任务与条目 ----

    def active_scopes(self) -> tuple[set[str], set[str]]:
        rows = self.connection.execute(
            "SELECT payloads_json,projects_json FROM maintenance_windows WHERE status='active'"
        ).fetchall()
        payloads: set[str] = set()
        projects: set[str] = set()
        for row in rows:
            payloads.update(json.loads(row["payloads_json"]))
            projects.update(json.loads(row["projects_json"]))
        return payloads, projects

    def affected_tasks(self, payloads: Iterable[str], projects: Iterable[str]) -> list[sqlite3.Row]:
        clauses: list[str] = []
        values: list[Any] = []
        payloads = list(payloads)
        projects = list(projects)
        if payloads:
            placeholders = ",".join("?" for _ in payloads)
            clauses.append(f"t.payload_code IN ({placeholders})")
            values.extend(payloads)
        if projects:
            placeholders = ",".join("?" for _ in projects)
            clauses.append(f"t.project_code IN ({placeholders})")
            values.extend(projects)
        if not clauses:
            return []
        return self.connection.execute(
            "SELECT t.*,tpl.code AS template_code FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id "
            "WHERE " + " OR ".join(clauses) + " ORDER BY t.queue_ticket,t.id",
            values,
        ).fetchall()

    def item_by_task(self, window_id: int, task_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM maintenance_window_items WHERE window_id=? AND task_id=?", (window_id, task_id),
        ).fetchone()

    def item_by_id(self, item_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM maintenance_window_items WHERE id=?", (item_id,)).fetchone()

    def insert_item(self, *, window_id: int, task: sqlite3.Row, category: str, drain_state: str, restore_state: str, now: str) -> int:
        snapshot = {key: task[key] for key in task.keys()}
        snapshot = {k: snapshot[k] for k in snapshot if k != "parameters_json"}
        cursor = self.connection.execute(
            """
            INSERT INTO maintenance_window_items(
                window_id,task_id,category,drain_state,restore_state,queue_ticket,snapshot_json,updated_at,created_at
            ) VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(window_id,task_id) DO NOTHING
            """,
            (window_id, task["id"], category, drain_state, restore_state, task["queue_ticket"],
             json.dumps(snapshot, ensure_ascii=False, sort_keys=True, default=str), now, now),
        )
        return int(cursor.lastrowid or 0)

    def touch_item(self, item_id: int, now: str, **fields: Any) -> None:
        if not fields:
            return
        assignments = ", ".join(f"{key}=?" for key in fields)
        params = [*fields.values(), now, item_id]
        self.connection.execute(
            f"UPDATE maintenance_window_items SET {assignments},updated_at=? WHERE id=?", params,
        )

    def list_items(self, window_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT i.*, t.status AS task_status, t.project_code, t.payload_code, t.priority AS task_priority
            FROM maintenance_window_items i
            JOIN compute_tasks t ON t.id=i.task_id
            WHERE i.window_id=? ORDER BY i.queue_ticket,i.id
            """,
            (window_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def items_in_drain_states(self, window_id: int, states: Iterable[str]) -> list[sqlite3.Row]:
        states = list(states)
        placeholders = ",".join("?" for _ in states)
        return self.connection.execute(
            f"SELECT * FROM maintenance_window_items WHERE window_id=? AND drain_state IN ({placeholders}) ORDER BY queue_ticket,id",
            [window_id, *states],
        ).fetchall()

    def items_in_restore_states(self, window_id: int, states: Iterable[str]) -> list[sqlite3.Row]:
        states = list(states)
        placeholders = ",".join("?" for _ in states)
        return self.connection.execute(
            f"SELECT * FROM maintenance_window_items WHERE window_id=? AND restore_state IN ({placeholders}) ORDER BY queue_ticket,id",
            [window_id, *states],
        ).fetchall()

    # ---- 检查点 ----

    def add_checkpoint(self, *, window_id: int, task_id: int, worker_id: str, checkpoint_data: dict[str, Any], progress_percent: float | None, created_by: str, now: str) -> dict[str, Any]:
        sequence = int(self.connection.execute(
            "SELECT COALESCE(MAX(sequence),0)+1 FROM task_checkpoints WHERE task_id=? AND window_id=?",
            (task_id, window_id),
        ).fetchone()[0])
        cursor = self.connection.execute(
            """
            INSERT INTO task_checkpoints(task_id,window_id,sequence,worker_id,checkpoint_data_json,progress_percent,created_by,created_at)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (task_id, window_id, sequence, worker_id,
             json.dumps(checkpoint_data, ensure_ascii=False, sort_keys=True), progress_percent, created_by, now),
        )
        row = self.connection.execute("SELECT * FROM task_checkpoints WHERE id=?", (cursor.lastrowid,)).fetchone()
        return dict(row)

    def latest_checkpoint(self, window_id: int, task_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM task_checkpoints WHERE window_id=? AND task_id=? ORDER BY sequence DESC LIMIT 1",
            (window_id, task_id),
        ).fetchone()

    def list_checkpoints(self, window_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute(
            "SELECT * FROM task_checkpoints WHERE window_id=? ORDER BY task_id,sequence", (window_id,),
        ).fetchall()]

    # ---- 恢复批次 ----

    def create_batch(self, *, window_id: int, batch_number: int, planned_at: str | None, now: str) -> int:
        cursor = self.connection.execute(
            "INSERT INTO maintenance_restore_batches(window_id,batch_number,status,planned_at,item_count,created_at) VALUES(?,?, 'planned',?,0,?)",
            (window_id, batch_number, planned_at, now),
        )
        return int(cursor.lastrowid)

    def touch_batch(self, batch_id: int, now: str, **fields: Any) -> None:
        del now
        if not fields:
            return
        assignments = ", ".join(f"{key}=?" for key in fields)
        params = [*fields.values(), batch_id]
        self.connection.execute(
            f"UPDATE maintenance_restore_batches SET {assignments} WHERE id=?", params,
        )

    def batch_by_number(self, window_id: int, batch_number: int) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM maintenance_restore_batches WHERE window_id=? AND batch_number=?",
            (window_id, batch_number),
        ).fetchone()

    def list_batches(self, window_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM maintenance_restore_batches WHERE window_id=? ORDER BY batch_number", (window_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    # ---- 审计事件 ----

    def add_event(self, *, window_id: int, task_id: int | None, event_type: str, actor: str, detail: dict[str, Any], now: str) -> None:
        self.connection.execute(
            "INSERT INTO maintenance_window_events(window_id,task_id,event_type,actor,detail_json,created_at) VALUES(?,?,?,?,?,?)",
            (window_id, task_id, event_type, actor,
             json.dumps(detail, ensure_ascii=False, sort_keys=True, default=str), now),
        )

    def list_events(self, window_id: int, *, event_type: str | None = None, task_id: int | None = None, limit: int = 200) -> list[dict[str, Any]]:
        clauses = ["window_id=?"]
        values: list[Any] = [window_id]
        if event_type:
            clauses.append("event_type=?")
            values.append(event_type)
        if task_id is not None:
            clauses.append("task_id=?")
            values.append(task_id)
        values.append(min(limit, 1000))
        rows = self.connection.execute(
            f"SELECT * FROM maintenance_window_events WHERE {' AND '.join(clauses)} ORDER BY id DESC LIMIT ?",
            values,
        ).fetchall()
        return [dict(row) for row in rows]
