"""Injected read transport and fake-only publication contracts.

No Elasticsearch client construction, credential loading, live publisher,
entrypoint, or scheduler registration is provided by this package.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import re
from typing import Any, AsyncIterator, Protocol

from apps.classifiers.source_networker import validate_source_networker

from .models import positive_id


class ReadTransport(Protocol):
    async def open_point_in_time(self, **kwargs: Any) -> Any: ...
    async def search(self, **kwargs: Any) -> Any: ...
    async def close_point_in_time(self, **kwargs: Any) -> Any: ...


class IncompleteRead(RuntimeError):
    """The caller must discard/retry the batch; do not advance its checkpoint."""


class ElasticsearchReadAdapter:
    """PIT-bounded reads through an explicitly injected transport; tested with fakes.

    A complete PIT scan only proves ES retrieval completeness, NOT NetWorker API
    coverage. No successful read is converted into a Coverage attestation here.
    """

    def __init__(self, transport: ReadTransport, *, page_size: int = 100, max_pages: int = 1000):
        if not 1 <= page_size <= 1000 or max_pages < 1:
            raise ValueError("invalid_read_bounds")
        self.transport, self.page_size, self.max_pages = transport, page_size, max_pages

    async def read_batch(self, indices: list[str], query: dict[str, Any]) -> list[dict[str, Any]]:
        """Stage a bounded complete batch; no partial list escapes on scan failure."""
        return [hit async for hit in self.scan(indices, query)]

    async def scan(self, indices: list[str], query: dict[str, Any]) -> AsyncIterator[dict[str, Any]]:
        pattern = r"(?:nw-ops-raw-(?:core|chnl|info)|nw-(?:core|chnl|info|unmapped)-job-run)-[0-9]{4}-(?:0[1-9]|1[0-2])"
        if not indices or any(not isinstance(i, str) or not re.fullmatch(pattern, i) for i in indices):
            raise ValueError("explicit_monthly_raw_or_job_run_indices_required")
        opened = await self.transport.open_point_in_time(index=indices, keep_alive="1m", allow_partial_search_results=False)
        pit_id = opened["id"]
        after = None
        try:
            if opened.get("_shards", {}).get("failed", 0):
                raise IncompleteRead("partial_pit_open")
            for _ in range(self.max_pages):
                args: dict[str, Any] = {"pit": {"id": pit_id, "keep_alive": "1m"},
                                        "query": deepcopy(query), "size": self.page_size,
                                        "sort": ["_shard_doc"], "track_total_hits": False,
                                        "allow_partial_search_results": False}
                if after is not None:
                    args["search_after"] = after
                response = await self.transport.search(**args)
                pit_id = response.get("pit_id", pit_id)
                if response.get("timed_out") or response.get("_shards", {}).get("failed", 0) or response.get("terminated_early"):
                    raise IncompleteRead("partial_or_timed_out_search")
                hits = response["hits"]["hits"]
                if not isinstance(hits, list):
                    raise IncompleteRead("malformed_hits")
                if not hits:
                    return
                next_after = hits[-1].get("sort")
                if not isinstance(next_after, list) or not next_after or next_after == after:
                    raise IncompleteRead("missing_or_repeated_page_cursor")
                for hit in hits:
                    if not isinstance(hit.get("_source"), dict):
                        raise IncompleteRead("source_unavailable")
                    yield deepcopy(hit)
                after = next_after
            raise IncompleteRead("page_budget_exhausted")
        finally:
            await self.transport.close_point_in_time(id=pit_id)


@dataclass(frozen=True)
class Checkpoint:
    """Opaque batch cursor; no wall-clock or API completeness is inferred."""

    cursor: str
    evidence_generation: str


class LookupStateStore(Protocol):
    def commit(self, documents: dict[str, dict[str, Any]], *, expected_version: int,
               checkpoint: Checkpoint) -> int: ...


class VersionConflict(RuntimeError):
    pass


class InMemoryLookupStore:
    """Test fake ONLY. Not durable, not thread-safe, and never an ES writer.

    Document state and checkpoint change together or not at all in this fake.
    Cross-process CAS, durable evidence ledger and restart recovery are deferred.
    """

    def __init__(self) -> None:
        self.version = 0
        self.documents: dict[str, dict[str, Any]] = {}
        self.checkpoint: Checkpoint | None = None

    def commit(self, documents: dict[str, dict[str, Any]], *, expected_version: int,
               checkpoint: Checkpoint) -> int:
        if expected_version != self.version:
            raise VersionConflict("stale_fake_state_version")
        for key, document in documents.items():
            validate_source_networker(document["source_networker"])
            if key != document["action_run_key"] or positive_id(document["action_job_id"]) is None:
                raise ValueError("invalid_lookup_document_identity")
            if key != f"{document['source_networker']}:{document['action_job_id']}":
                raise ValueError("lookup_key_mismatch")
            if document["mapping_status"] != "resolved" or document["verification_status"] != "verified":
                if any(document[name] is not None for name in ["policy_name", "workflow_name", "protection_group"]):
                    raise ValueError("unverified_public_relationship")
            elif document["candidate_count"] != 1 or not all(
                isinstance(document[name], str) and document[name]
                for name in ["policy_name", "workflow_name", "protection_group"]
            ):
                raise ValueError("invalid_verified_relationship")
            old = self.documents.get(key)
            if old:
                if (old["action_start_time"] and document["action_start_time"] != old["action_start_time"]
                        and document["verification_status"] != "blocked"):
                    raise ValueError("execution_replacement_requires_review")
                if old["candidate_count"] > document["candidate_count"] or (
                    old["candidate_count"] == document["candidate_count"]
                    and old["candidate_fingerprint"] != document["candidate_fingerprint"]
                ):
                    raise ValueError("incomplete_or_replaced_candidate_ledger")
                if old["candidate_count"] and old["candidate_fingerprint"] != document["candidate_fingerprint"]:
                    previous = {tuple(item["normalized"]) for item in old["candidate_evidence"]["items"]}
                    incoming = {tuple(item["normalized"]) for item in document["candidate_evidence"]["items"]}
                    if len(previous) != old["candidate_count"] or not previous.issubset(incoming):
                        raise ValueError("incomplete_or_replaced_candidate_ledger")
                if (old["action_status"] in {"success", "failed", "canceled"} and document["action_status"] == "running"
                        and document["verification_status"] != "blocked"):
                    raise ValueError("terminal_state_regression")
        # Fake transaction: retain untouched keys during a partial batch.
        replacement = deepcopy(self.documents)
        replacement.update(deepcopy(documents))
        for key, incoming in documents.items():
            old = self.documents.get(key)
            if old and incoming["verification_status"] == "blocked":
                # Revoke public names on conflict without replacing the prior
                # execution's metadata with a possibly reused ID's latest run.
                fields = ["action_start_time", "action_end_time", "action_status",
                          "action_job_type", "action_workflow_run_key"]
                replacement[key]["candidate_evidence"]["conflicting_execution"] = {
                    name: incoming[name] for name in fields
                }
                for name in fields:
                    replacement[key][name] = old[name]
        self.documents = replacement
        self.checkpoint = deepcopy(checkpoint)
        self.version += 1
        return self.version
