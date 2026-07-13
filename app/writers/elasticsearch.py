from __future__ import annotations

import logging
from typing import Any

from app.core.config import ElasticsearchConfig
from app.core.metrics import ELASTICSEARCH_WRITE_TOTAL
from app.models import CollectionResult

LOGGER = logging.getLogger(__name__)


class ElasticsearchWriter:
    def __init__(self, config: ElasticsearchConfig) -> None:
        self.config = config
        self.client: Any | None = None
        self._async_bulk: Any | None = None
        if config.is_ready:
            from elasticsearch import AsyncElasticsearch
            from elasticsearch.helpers import async_bulk

            basic_auth = (config.username, config.password) if config.username and config.password else None
            self.client = AsyncElasticsearch(
                hosts=list(config.hosts),
                basic_auth=basic_auth,
                verify_certs=config.verify_certs,
            )
            self._async_bulk = async_bulk
        elif config.enabled:
            LOGGER.warning("Elasticsearch is enabled but contains TO_BE_FILLED values; writes will be skipped")

    async def write_many(self, results: list[CollectionResult]) -> None:
        if not results:
            return
        if not self.client:
            ELASTICSEARCH_WRITE_TOTAL.labels("skipped").inc()
            return

        actions = [action for result in results for action in self._actions_for_result(result)]
        try:
            await self._async_bulk(self.client, actions)
            ELASTICSEARCH_WRITE_TOTAL.labels("success").inc()
        except Exception:
            LOGGER.exception("Failed to write collector results to Elasticsearch")
            ELASTICSEARCH_WRITE_TOTAL.labels("error").inc()

    async def close(self) -> None:
        if self.client:
            await self.client.close()

    def _actions_for_result(self, result: CollectionResult) -> list[dict]:
        return [
            {
                "_op_type": "index",
                "_index": self._index_name(result, "raw"),
                "_id": self._raw_document_id(result),
                "_source": self._raw_document(result),
            },
            {
                "_op_type": "index",
                "_index": self._index_name(result, "current"),
                "_id": self._current_document_id(result),
                "_source": self._current_document(result),
            },
        ]

    def _index_name(self, result: CollectionResult | None = None, document_type: str | None = None) -> str:
        if result and document_type in {"raw", "current"}:
            month_suffix = result.collected_at.strftime("%Y.%m")
            return f"backup-{document_type}-{self._solution(result)}-{month_suffix}"

        if result:
            month_suffix = result.collected_at.strftime("%Y.%m")
            return f"{self.config.index_prefix}-{self._solution(result)}-{month_suffix}"

        return self.config.index_prefix

    def _raw_document(self, result: CollectionResult) -> dict:
        document = result.to_document()
        return document | {
            "raw_document_id": self._raw_document_id(result),
            "solution": self._solution(result),
            "document_family": "raw",
            "document_type": "collection",
            "processing_mode": "elt",
        }

    def _current_document(self, result: CollectionResult) -> dict:
        payload = result.payload or {}
        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        return {
            "@timestamp": result.to_document()["@timestamp"],
            "current_document_id": self._current_document_id(result),
            "collector": result.collector,
            "target_type": result.target_type,
            "solution": self._solution(result),
            "protocol": result.protocol,
            "ok": result.ok,
            "error": result.error,
            "skipped": result.skipped,
            "skip_reason": result.skip_reason,
            "document_family": "current",
            "document_type": "status",
            "processing_mode": "etl",
            "summary": summary,
        }

    def _raw_document_id(self, result: CollectionResult) -> str:
        timestamp = result.collected_at.strftime("%Y%m%dT%H%M%S.%fZ")
        return f"{result.collector}:raw:{timestamp}"

    def _current_document_id(self, result: CollectionResult) -> str:
        return f"{result.collector}:current"

    @staticmethod
    def _solution(result: CollectionResult) -> str:
        aliases = {
            "DD": "dd",
            "DXi": "dxi",
            "i6000": "i6000",
            "Networker": "networker",
            "ZFS": "zfs",
        }
        return aliases.get(result.target_type, result.target_type.lower())
