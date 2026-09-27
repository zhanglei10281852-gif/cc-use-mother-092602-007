from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

WindowType = Literal["attitude_adjust", "radiator_maintenance", "other"]
DrainStrategy = Literal["graceful", "immediate", "pause_new"]


class WindowCreate(BaseModel):
    code: str = Field(min_length=2, max_length=64, pattern=r"^[a-z0-9][a-z0-9._-]+$")
    title: str = Field(min_length=2, max_length=120)
    window_type: WindowType
    payloads: list[str] = Field(min_length=1, max_length=200)
    drain_strategy: DrainStrategy = "graceful"
    starts_at: str = Field(min_length=5, max_length=40, description="ISO 8601 时间")
    ends_at: str = Field(min_length=5, max_length=40)
    grace_period_seconds: int = Field(default=0, ge=0, le=86400)
    restore_batch_size: int = Field(default=50, ge=1, le=1000)
    restore_interval_seconds: int = Field(default=0, ge=0, le=86400)
    created_by: str = Field(min_length=1, max_length=120)


class ActivateRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)


class ExtendRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)
    new_ends_at: str | None = Field(default=None, min_length=5, max_length=40)
    extra_seconds: int | None = Field(default=None, ge=1, le=7 * 86400)

    @model_validator(mode="after")
    def require_one_target(self) -> "ExtendRequest":
        if self.new_ends_at is None and self.extra_seconds is None:
            raise ValueError("必须提供 new_ends_at 或 extra_seconds")
        return self


class CancelRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)


class CheckpointRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    checkpoint: dict[str, Any] = Field(default_factory=dict)


class ForceCheckpointRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)
    checkpoint: dict[str, Any] = Field(default_factory=dict)


class SkipItemRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)


class RestoreItemRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)


class ReleaseBatchRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
