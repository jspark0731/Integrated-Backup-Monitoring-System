from __future__ import annotations

from datetime import datetime, timezone
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
        if result.target_type == "i6000" and result.protocol == "rest":
            return [
                {
                    "_index": self._index_name(result, document_type),
                    "_source": result.to_document() | {"document_type": document_type},
                }
                for document_type in ("status", "drive", "media")
            ]
        if result.target_type == "Networker" and result.protocol == "rest":
            actions = []
            for document_type, payload_key in (
                ("job", "jobs"),
                ("client", "clients"),
                ("policy", "policies"),
                ("workflow", "workflows"),
                ("monthly-report", "monthly_report"),
            ):
                records = result.payload.get(payload_key) if result.payload else None
                if not isinstance(records, list):
                    continue
                for record in records:
                    actions.append(
                        {
                            "_index": self._index_name(result, document_type),
                            "_source": result.to_document()
                            | {
                                "document_type": document_type,
                                "payload": record,
                            },
                        }
                    )
            if actions:
                return actions
        if result.target_type == "ZFS" and result.protocol == "rest":
            actions = []
            summary = result.payload.get("summary") if result.payload else None
            if isinstance(summary, dict):
                actions.append(
                    {
                        "_index": self._index_name(result, "summary"),
                        "_source": result.to_document()
                        | {
                            "document_type": "summary",
                            "payload": summary,
                        },
                    }
                )
            for document_type, payload_key in (
                ("pool", "pools"),
                ("status", "alerts"),
            ):
                records = result.payload.get(payload_key) if result.payload else None
                if not isinstance(records, list):
                    continue
                for record in records:
                    actions.append(
                        {
                            "_index": self._index_name(result, document_type),
                            "_source": result.to_document()
                            | {
                                "document_type": document_type,
                                "payload": record,
                            },
                        }
                    )
            if actions:
                return actions
        return [
            {
                "_index": self._index_name(result),
                "_source": result.to_document(),
            }
        ]

    def _index_name(self, result: CollectionResult | None = None, document_type: str | None = None) -> str:
        if result and result.target_type == "DXi":
            month_suffix = datetime.now(timezone.utc).strftime("%Y.%m")
            if result.protocol in {"ssh", "cli"}:
                return f"backup-dxi-summary-{month_suffix}"
            return f"backup-dxi-status-{month_suffix}"

        if result and result.target_type == "i6000":
            month_suffix = datetime.now(timezone.utc).strftime("%Y.%m")
            if result.protocol == "rest" and document_type:
                return f"backup-i6000-{document_type}-{month_suffix}"
            return f"backup-i6000-status-{month_suffix}"

        if result and result.target_type == "Networker":
            month_suffix = datetime.now(timezone.utc).strftime("%Y.%m")
            if result.protocol == "rest" and document_type:
                return f"backup-networker-{document_type}-{month_suffix}"
            return f"backup-networker-status-{month_suffix}"

        if result and result.target_type == "ZFS":
            month_suffix = datetime.now(timezone.utc).strftime("%Y.%m")
            if result.protocol == "rest" and document_type:
                return f"backup-zfs-{document_type}-{month_suffix}"
            return f"backup-zfs-status-{month_suffix}"

        date_suffix = datetime.now(timezone.utc).strftime("%Y.%m.%d")
        return f"{self.config.index_prefix}-{date_suffix}"
