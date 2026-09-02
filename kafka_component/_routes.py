from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter


def build_health_router(get_status: Callable[[], dict[str, Any]]) -> APIRouter:
    router = APIRouter()

    @router.get("/health")
    async def health() -> dict[str, Any]:
        return get_status()

    return router
