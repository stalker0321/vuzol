import asyncio

from httpx import ASGITransport, AsyncClient

from vuzol.app import create_app
from vuzol.config import Settings


def test_health_endpoints_report_ready() -> None:
    app = create_app(Settings(environment="test", service_name="test-vuzol"))

    async def exercise_app() -> None:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            for path in ("/health/live", "/health/ready"):
                response = await client.get(path)
                assert response.status_code == 200
                assert response.json() == {
                    "status": "ok",
                    "service": "test-vuzol",
                    "environment": "test",
                }

    asyncio.run(exercise_app())
