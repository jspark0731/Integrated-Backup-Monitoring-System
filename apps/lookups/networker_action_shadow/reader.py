"""HTTP-based PIT reads. No source mutation and no NetWorker requests."""

from copy import deepcopy
from dataclasses import dataclass
import re

from apps.lookups.networker_action.adapters import IncompleteRead
from apps.lookups.networker_action.models import timestamp

DEFAULT_JOB_START = "2026-11-09T03:00:00Z"
DEFAULT_JOB_END = "2026-11-09T05:30:00Z"


def validate_indices(indices, channel):
    patterns = {
        "jobs": r"(?:nw-(?:core|chnl|info|unmapped)-job-run-\d{4}-(?:0[1-9]|1[0-2])|tmp-nw-job-run-4c2-\d{8})",
        "raw": r"nw-ops-raw-(?:core|chnl|info)-\d{4}-(?:0[1-9]|1[0-2])",
        "shadow": r"poc-nw-s4c2-action-lookup",
    }
    if channel not in patterns or not indices or len(set(indices)) != len(indices) or any(
        not isinstance(i, str) or not re.fullmatch(patterns[channel], i) for i in indices
    ):
        raise ValueError("explicit_input_indices_required")


def range_query(source, channel, start, end):
    from apps.classifiers.source_networker import validate_source_networker
    validate_source_networker(source)
    if channel not in {"jobs", "raw"} or timestamp(start) is None or timestamp(end) is None or timestamp(start) >= timestamp(end):
        raise ValueError("invalid_aware_read_window")
    time_field = "observed_at" if channel == "jobs" else "@timestamp"
    nested = "payload.source_networker" if channel == "jobs" else "payload.summary.source_networker"
    filters = [
        {"range": {time_field: {"gte": timestamp(start).isoformat(), "lt": timestamp(end).isoformat()}}},
        {"bool": {"should": [{"term": {"source_networker": source}}, {"term": {nested: source}}], "minimum_should_match": 1}},
        {"term": {"document_type": "job-run"}} if channel == "jobs" else {"term": {"collection_class": "backups"}},
    ]
    return {"bool": {"filter": filters}}


@dataclass
class ReadAudit:
    pages: int = 0
    hits: int = 0
    expected_total: int | None = None
    terminal_empty: bool = False
    pit_closed: bool = False
    complete: bool = False


class PITReader:
    """Strict complete scan; errors discard the channel, never resume an expired PIT."""

    def __init__(self, http, *, page_size=100, max_pages=10000, max_hits=100000):
        if not 1 <= page_size <= 1000 or min(max_pages, max_hits) < 1:
            raise ValueError("invalid_read_limits")
        self.http, self.page_size, self.max_pages, self.max_hits = http, page_size, max_pages, max_hits
        self.audit = ReadAudit()

    async def preflight(self, indices, channel):
        validate_indices(indices, channel)
        nested = "payload.source_networker" if channel == "jobs" else "payload.summary.source_networker"
        time_field = "observed_at" if channel == "jobs" else "@timestamp"
        kind = "document_type" if channel == "jobs" else "collection_class"
        fields = [time_field, kind, "source_networker", nested]
        response = await self.http.request("GET", f"/{','.join(indices)}/_field_caps",
                                           params={"fields": ",".join(fields), "include_unmapped": "true"})
        caps = response.get("fields", {})

        def usable(name, allowed):
            entries = caps.get(name, {})
            return bool(entries) and all(t in allowed and v.get("searchable") is True
                                         and not v.get("non_searchable_indices") for t, v in entries.items())

        if not usable(time_field, {"date", "date_nanos"}) or not usable(kind, {"keyword"}) or not any(
            usable(f, {"keyword"}) for f in ["source_networker", nested]
        ):
            raise IncompleteRead("input_mapping_not_searchable")

    async def read(self, indices, channel, query):
        validate_indices(indices, channel)
        self.audit = ReadAudit()
        opened = await self.http.request("POST", f"/{','.join(indices)}/_pit",
                                         params={"keep_alive": "2m", "allow_partial_search_results": "false"})
        pit = opened.get("id")
        if not isinstance(pit, str) or not pit:
            raise IncompleteRead("missing_pit_id")
        hits_out, seen, after = [], set(), None
        try:
            if opened.get("_shards", {}).get("failed", 0):
                raise IncompleteRead("partial_pit_open")
            for page in range(self.max_pages):
                body = {"pit": {"id": pit, "keep_alive": "2m"}, "query": deepcopy(query),
                        "sort": ["_shard_doc"], "size": self.page_size, "track_total_hits": page == 0}
                if after is not None:
                    body["search_after"] = after
                response = await self.http.request("POST", "/_search", json=body,
                                                   params={"allow_partial_search_results": "false"})
                pit = response.get("pit_id", pit)
                self.audit.pages += 1
                if not isinstance(pit, str) or not pit or response.get("timed_out") or response.get("terminated_early") or response.get("_shards", {}).get("failed", 0):
                    raise IncompleteRead("partial_search")
                clusters = response.get("_clusters", {})
                if any(clusters.get(k, 0) for k in ["skipped", "failed", "partial", "running"]):
                    raise IncompleteRead("partial_clusters")
                container = response.get("hits", {})
                if page == 0:
                    total = container.get("total", {})
                    if total.get("relation") != "eq" or type(total.get("value")) is not int or total["value"] < 0:
                        raise IncompleteRead("exact_total_required")
                    self.audit.expected_total = total["value"]
                hits = container.get("hits")
                if not isinstance(hits, list):
                    raise IncompleteRead("malformed_hits")
                if not hits:
                    self.audit.terminal_empty = True
                    if self.audit.hits != self.audit.expected_total:
                        raise IncompleteRead("total_mismatch")
                    break
                for hit in hits:
                    if not isinstance(hit, dict) or not isinstance(hit.get("_source"), dict):
                        raise IncompleteRead("source_unavailable")
                    identity = (hit.get("_index"), hit.get("_id"))
                    cursor = hit.get("sort")
                    if identity[0] not in indices or not isinstance(identity[1], str) or not identity[1] or identity in seen:
                        raise IncompleteRead("duplicate_or_out_of_scope_hit")
                    if not isinstance(cursor, list) or len(cursor) != 1 or type(cursor[0]) is not int or (after is not None and cursor[0] <= after[0]):
                        raise IncompleteRead("invalid_or_reversed_cursor")
                    after = cursor
                    seen.add(identity)
                    hits_out.append(deepcopy(hit))
                    self.audit.hits += 1
                    if self.audit.hits > self.max_hits:
                        raise IncompleteRead("hit_budget_exhausted")
            else:
                raise IncompleteRead("page_budget_exhausted")
        finally:
            closed = await self.http.request("DELETE", "/_pit", json={"id": pit})
            self.audit.pit_closed = closed.get("succeeded") is True
            if not self.audit.pit_closed:
                raise IncompleteRead("pit_close_failed")
        self.audit.complete = True
        return hits_out
