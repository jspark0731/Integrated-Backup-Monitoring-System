from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
import os

import uvicorn
from fastapi import FastAPI

from app.api.routes import router
from app.collectors.factory import build_collector
from app.core.config import load_config
from app.core.logging import configure_logging
from app.scheduler import CollectorScheduler
from app.writers.elasticsearch import ElasticsearchWriter


CONFIG_PATH = Path(os.getenv("APP_CONFIG", "config/collector.example.yaml"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = load_config(CONFIG_PATH)
    configure_logging(config.log_level)

    collectors = [build_collector(item) for item in config.collectors]
    writer = ElasticsearchWriter(config.elasticsearch)
    scheduler = CollectorScheduler(collectors, writer)
    app.state.config = config
    app.state.scheduler = scheduler

    await scheduler.start()
    try:
        yield
    finally:
        await scheduler.stop()


app = FastAPI(title="Backup Dashboard Collector", version="0.1.0", lifespan=lifespan)
app.include_router(router)


def run() -> None:
    uvicorn.run("app.main:app", host="0.0.0.0", port=8080)


if __name__ == "__main__":
    run()

