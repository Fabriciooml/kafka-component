from __future__ import annotations

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from kafka_component._routes import build_health_router


async def test_health_router_returns_status_from_callback():
    def get_status() -> dict:
        return {"connected": True, "last_error": None}

    app = FastAPI()
    app.include_router(build_health_router(get_status))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"connected": True, "last_error": None}
