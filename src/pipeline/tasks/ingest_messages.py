"""Ingest conversation messages from CSV."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import List

from src.db.schemas import MessageRecord
from src.utils.logging import logger


def _ensure_columns(row: dict[str, str], idx: int) -> dict[str, str]:
    required = {"user_id", "campaign_id", "message", "timestamp"}
    missing = required - row.keys()
    if missing:
        raise ValueError(f"Row {idx} missing required columns: {missing}")
    row.setdefault("message_id", f"msg_{idx}")
    return row


def load_conversations(path: str | Path) -> List[MessageRecord]:
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Conversation data not found at {file_path}")

    records: List[MessageRecord] = []
    with file_path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for idx, row in enumerate(reader, start=1):
            cleaned = _ensure_columns(row, idx)
            try:
                record = MessageRecord(**cleaned)
            except Exception as exc:  # pylint: disable=broad-except
                logger.error("invalid_message_record", row=cleaned, error=str(exc))
                continue
            records.append(record)

    logger.bind(count=len(records), file=str(file_path)).info("conversations_loaded")
    return records


__all__ = ["load_conversations"]
