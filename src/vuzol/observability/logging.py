"""Structured JSON logging without secret-bearing configuration dumps."""

import json
import logging
from datetime import UTC, datetime
from typing import Any

_STANDARD_RECORD_FIELDS = frozenset(logging.makeLogRecord({}).__dict__)


class ServiceFilter(logging.Filter):
    """Attach the process service identity to every record."""

    def __init__(self, service: str) -> None:
        super().__init__()
        self._service = service

    def filter(self, record: logging.LogRecord) -> bool:
        record.service = self._service
        return True


class JsonFormatter(logging.Formatter):
    """Render stable structured fields and explicitly supplied context as JSON."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "severity": record.levelname,
            "service": getattr(record, "service", "vuzol"),
            "event": getattr(record, "event", record.name),
            "message": record.getMessage(),
        }
        for field in ("correlation_id", "task_id"):
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value

        for key, value in record.__dict__.items():
            if (
                key not in _STANDARD_RECORD_FIELDS
                and key not in payload
                and not key.startswith("_")
            ):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, separators=(",", ":"))


def configure_logging(*, service: str, level: str) -> None:
    """Configure the root logger for one process."""

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    handler.addFilter(ServiceFilter(service))
    logging.basicConfig(level=level, handlers=[handler], force=True)
    # HTTP client request URLs can contain provider credentials (Telegram embeds the bot token).
    logging.getLogger("httpx").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Return a module logger."""

    return logging.getLogger(name)
