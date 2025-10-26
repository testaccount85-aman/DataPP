"""Logging utilities using Loguru with structured JSON output."""
from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from typing import Iterator

from loguru import logger

_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logger.remove()
logger.add(
    sys.stdout,
    serialize=True,
    colorize=False,
    backtrace=False,
    diagnose=False,
    level=_LOG_LEVEL,
)


@contextmanager
def log_context(**extra: object) -> Iterator[None]:
    """Context manager to temporarily bind extra fields."""

    with logger.contextualize(**extra):
        yield


__all__ = ["logger", "log_context"]
