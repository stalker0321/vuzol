import json
import logging

from vuzol.observability.logging import JsonFormatter, ServiceFilter, configure_logging, get_logger


def test_json_formatter_adds_contract_fields() -> None:
    record = logging.LogRecord(
        name="vuzol.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="task accepted",
        args=(),
        exc_info=None,
    )
    record.service = "worker"
    record.event = "task.accepted"
    record.task_id = "task-123"
    record.correlation_id = "correlation-456"

    payload = json.loads(JsonFormatter().format(record))

    assert payload["severity"] == "INFO"
    assert payload["service"] == "worker"
    assert payload["event"] == "task.accepted"
    assert payload["message"] == "task accepted"
    assert payload["task_id"] == "task-123"
    assert payload["correlation_id"] == "correlation-456"
    assert payload["timestamp"].endswith("+00:00")


def test_service_filter_attaches_service() -> None:
    record = logging.makeLogRecord({})

    assert ServiceFilter("test-service").filter(record)
    assert record.__dict__["service"] == "test-service"


def test_configure_logging_sets_json_handler() -> None:
    configure_logging(service="test-service", level="WARNING")

    logger = get_logger("vuzol.test.configured")
    root = logging.getLogger()

    assert logger.name == "vuzol.test.configured"
    assert root.level == logging.WARNING
    assert isinstance(root.handlers[0].formatter, JsonFormatter)
    assert logging.getLogger("httpx").level == logging.WARNING
