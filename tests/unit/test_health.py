from vuzol.app.health import health_status


def test_health_status_is_ready() -> None:
    result = health_status(service="test-service", environment="test")

    assert result.model_dump() == {
        "status": "ok",
        "service": "test-service",
        "environment": "test",
    }
