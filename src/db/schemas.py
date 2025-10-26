"""Database schemas and Pydantic models."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, validator


class MessageRecord(BaseModel):
    user_id: str = Field(..., description="Unique user identifier")
    campaign_id: str = Field(..., description="Associated campaign identifier")
    message: str = Field(..., description="Raw message text")
    timestamp: str = Field(..., description="ISO8601 timestamp")
    message_id: str = Field(..., description="Unique message id")

    @validator("timestamp")
    def validate_ts(cls, v: str) -> str:
        datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v


SQLITE_DDL: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS engagement (
        user_id TEXT NOT NULL,
        campaign_id TEXT NOT NULL,
        ts TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS campaign_stats (
        campaign_id TEXT PRIMARY KEY,
        impressions INTEGER DEFAULT 0,
        clicks INTEGER DEFAULT 0,
        engagements INTEGER DEFAULT 0
    )
    """,
)


def as_sqlite_row(record: MessageRecord) -> tuple[str, str, str]:
    return record.user_id, record.campaign_id, record.timestamp


def update_campaign_stats_sql() -> str:
    return (
        "INSERT INTO campaign_stats(campaign_id, engagements) "
        "SELECT campaign_id, COUNT(*) FROM engagement GROUP BY campaign_id "
        "ON CONFLICT(campaign_id) DO UPDATE SET engagements=excluded.engagements"
    )


__all__ = [
    "MessageRecord",
    "SQLITE_DDL",
    "as_sqlite_row",
    "update_campaign_stats_sql",
]
