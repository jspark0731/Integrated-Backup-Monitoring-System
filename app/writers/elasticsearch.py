from __future__ import annotations

import logging
from typing import Any

from app.core.config import ElasticsearchConfig
from app.core.metrics import ELASTICSEARCH_WRITE_TOTAL
from app.models import CollectionResult
from app.processors.derived import build_derived_documents

LOGGER = logging.getLogger(__name__)

SITE_ALIASES = {
    "core": "CORE",
    "chnl": "CHNL",
    "info": "INFO",
    "ifrs": "IFRS",
}


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
                ca_certs=config.ca_certs,
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
        if result.target_type == "Networker":
            return self._networker_actions(result)
        actions = [
            {
                "_op_type": "index",
                "_index": self._index_name(result, "raw"),
                "_id": self._raw_document_id(result),
                "_source": self._raw_document(result),
            }
        ]
        if result.collection_class == "fast":
            actions.append(
                {
                    "_op_type": "index",
                    "_index": self._index_name(result, "current"),
                    "_id": self._current_document_id(result),
                    "_source": self._current_document(result),
                }
            )
        return actions

    def _networker_actions(self, result: CollectionResult) -> list[dict]:
        raw_document = self._raw_document(result)
        source = self._networker_source(result)
        month = result.collected_at.strftime("%Y-%m")
        raw_index = f"NW-OPS-RAW-{source.upper()}-{month}"
        actions = [
            {
                "_op_type": "index",
                "_index": raw_index,
                "_id": self._raw_document_id(result),
                "_source": raw_document,
            }
        ]
        if result.collection_class == "fast":
            actions.append(
                {
                    "_op_type": "index",
                    "_index": f"NW-OPS-CURRENT-{source.upper()}",
                    "_id": self._current_document_id(result),
                    "_source": self._current_document(result),
                }
            )
        for document in build_derived_documents(raw_document):
            domain = self._security_domain(document)
            document_type = self._networker_entity_segment(document)
            actions.append(
                {
                    "_op_type": "index",
                    "_index": f"NW-{domain.upper()}-{document_type}-{month}",
                    "_id": document["derived_id"],
                    "_source": document,
                }
            )
        return actions

    def _index_name(
        self,
        result: CollectionResult | None = None,
        document_type: str | None = None,
    ) -> str:
        if result:
            family = self._index_family(result)
            if document_type == "current":
                return f"{family}-CURRENT"
            month = result.collected_at.strftime("%Y-%m")
            return f"{family}-RAW-{month}"

        return self.config.index_prefix

    def _raw_document(self, result: CollectionResult) -> dict:
        document = result.to_document()
        return document | {
            "raw_document_id": self._raw_document_id(result),
            "device_name": self._device_name(result),
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
            "device_name": self._device_name(result),
            "target_type": result.target_type,
            "solution": self._solution(result),
            "protocol": result.protocol,
            "ok": result.ok,
            "error": result.error,
            "skipped": result.skipped,
            "skip_reason": result.skip_reason,
            "collection_class": result.collection_class,
            "collection_status": payload.get(
                "collection_status",
                "success" if result.ok else "error",
            ),
            "endpoint_errors": payload.get("endpoint_errors", {}),
            "document_family": "current",
            "document_type": "status",
            "processing_mode": "etl",
            "summary": summary,
        }

    @staticmethod
    def _networker_source(result: CollectionResult) -> str:
        summary = result.payload.get("summary") if isinstance(result.payload, dict) else {}
        if isinstance(summary, dict) and summary.get("source_networker"):
            source = str(summary["source_networker"]).strip().lower()
        else:
            source = _site_segment(result.collector).lower()
        return source if source in {"core", "chnl", "info", "ifrs"} else "unknown"

    @staticmethod
    def _security_domain(document: dict) -> str:
        payload = document.get("payload")
        domain = payload.get("security_domain") if isinstance(payload, dict) else None
        normalized = str(domain or "unmapped").strip().lower()
        return normalized if normalized in {"core", "chnl", "info", "ifrs"} else "unmapped"

    @staticmethod
    def _networker_entity_segment(document: dict) -> str:
        document_type = str(document.get("document_type") or "unknown").strip().lower()
        aliases = {"monthly-report": "MONTHLY"}
        return aliases.get(document_type, document_type.upper())

    def _raw_document_id(self, result: CollectionResult) -> str:
        timestamp = result.collected_at.strftime("%Y%m%dT%H%M%S.%fZ")
        return f"{result.collector}:raw:{timestamp}"

    def _current_document_id(self, result: CollectionResult) -> str:
        return f"{result.collector}:current"

    @staticmethod
    def _solution(result: CollectionResult) -> str:
        aliases = {
            "DD": "vtl",
            "DXi": "vtl",
            "i6000": "ptl",
            "Networker": "networker",
            "ZFS": "zfs",
        }
        return aliases.get(result.target_type, result.target_type.lower())

    @staticmethod
    def _index_family(result: CollectionResult) -> str:
        aliases = {
            "DD": "VTL",
            "DXi": "VTL",
            "i6000": "PTL",
            "Networker": "NW",
            "ZFS": "ZFS",
        }
        return aliases.get(result.target_type, result.target_type.upper())

    @staticmethod
    def _device_name(result: CollectionResult) -> str:
        payload = result.payload if isinstance(result.payload, dict) else {}
        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        for key in ("device_name", "server", "name"):
            value = summary.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
        return result.collector

def _site_segment(collector: str) -> str:
    normalized = collector.lower().replace("-", "_")
    for token, segment in SITE_ALIASES.items():
        if token in normalized.split("_"):
            return segment
    return collector.upper()
