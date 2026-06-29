from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

router = APIRouter()


@router.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}


@router.get("/readyz")
async def readyz(request: Request) -> dict:
    scheduler = request.app.state.scheduler
    return {
        "status": "ready",
        "collectors": len(scheduler.collectors),
    }


@router.get("/collectors")
async def collectors(request: Request) -> list[dict]:
    scheduler = request.app.state.scheduler
    return [
        {
            "name": collector.name,
            "type": collector.target_type,
            "protocol": collector.protocol,
            "enabled": collector.config.enabled,
            "schedule_second": collector.config.schedule_second,
            "skip_reason": collector.config.skip_reason,
            "last_result": scheduler.last_results.get(collector.name).to_document()
            if collector.name in scheduler.last_results
            else None,
        }
        for collector in scheduler.collectors
    ]


@router.post("/collectors/run-once")
async def run_once(request: Request) -> list[dict]:
    scheduler = request.app.state.scheduler
    results = await scheduler.run_once()
    return [result.to_document() for result in results]


@router.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

