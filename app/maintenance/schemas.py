from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, model_validator


class WindowCreate(BaseModel):
    code: str = Field(min_length=2, max_length=64, pattern=r"^[a-z0-9][a-z0-9._-]+$")
    title: str = Field(min_length=2, max_length=120)
    window_type: Literal["attitude_adjust", "radiator_maintenance", "other"] = "other"
    payloads: list[str] = Field(default_factory=list, max_length=200)
    projects: list[str] = Field(default_factory=list, max_length=200)
    drain_strategy: Literal["graceful", "immediate", "checkpoint_only"] = "graceful"
    restore_batch_size: int = Field(default=10, ge=1, le=500)
    restore_interval_seconds: int = Field(default=60, ge=0, le=86400)
    restore_order: Literal["committed", "priority"] = "committed"
    planned_start_at: datetime
    planned_end_at: datetime
    created_by: str = Field(min_length=1, max_length=120)

    @model_validator(mode="after")
    def validate_scope_and_window(self) -> "WindowCreate":
        if not self.payloads and not self.projects:
            raise ValueError("至少指定一个受影响载荷或项目")
        if self.planned_end_at <= self.planned_start_at:
            raise ValueError("窗口结束时间必须晚于开始时间")
        return self


class ActivateRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    force: bool = False


class ExtendRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)
    new_end_at: datetime | None = None
    extend_seconds: int | None = Field(default=None, ge=1, le=86400 * 30)

    @model_validator(mode="after")
    def require_one_target(self) -> "ExtendRequest":
        if self.new_end_at is None and self.extend_seconds is None:
            raise ValueError("必须提供 new_end_at 或 extend_seconds")
        return self


class CancelWindowRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)


class EndDrainRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    # 仍有运行中任务未交检查点时，是否强制结束（这些任务将被跳过、不参与恢复）
    force: bool = False


class CheckpointSubmit(BaseModel):
    task_id: int = Field(ge=1)
    worker_id: str = Field(min_length=1, max_length=120)
    checkpoint_data: dict = Field(default_factory=dict)
    progress_percent: float | None = Field(default=None, ge=0, le=100)


class BatchRelease(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    batch_number: int | None = Field(default=None, ge=1)
    # 为 True 时仅释放 planned_at 已到期的下一批，batch_number 被忽略
    only_due: bool = False


class ItemSkip(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)


class ItemRestore(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)


class ExceptionReport(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    note: str = Field(min_length=2, max_length=2000)


class ExceptionResolve(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    note: str = Field(default="", max_length=2000)
