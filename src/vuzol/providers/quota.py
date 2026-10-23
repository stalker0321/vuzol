"""Versioned extensible quota windows; independent of execution admission."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class QuotaWindow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    id: Literal["rolling", "weekly", "monthly"]
    used_percent: float = Field(ge=0, le=100)
    reset_at: datetime
    status: Literal["ok", "rate-limited"]
    scope_id: str = Field(min_length=1, max_length=160)

    @field_validator("used_percent", mode="before")
    @classmethod
    def numeric_percent(cls, value: object) -> object:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("percentage must be a number")
        return value

    @field_validator("reset_at")
    @classmethod
    def aware_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("reset time must carry timezone")
        return value

    @property
    def remaining_percent(self) -> float:
        return 100.0 - self.used_percent
