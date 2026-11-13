"""Read complete ES snapshots and publish daily current through the existing writer."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
import re

from elasticsearch import ConflictError, NotFoundError
from elasticsearch.helpers import BulkIndexError

from apps.core.config import NetworkerCurrentConfig
from apps.parsers.networker_rest_parser import parse_networker_rest_payload
from apps.processors.networker_current import RESULTS, build_daily, closed_days, discovered_days, digest, timestamp, window

FIELDS = ["source_networker", "document_type", "payload.type", "payload.end_time", "payload.parent_job_id", "payload.root_parent_job_id"]


def job_indices(source, include_unmapped=True):
    if source not in {"core", "chnl", "info"}:
        raise ValueError("invalid current source")
    indices = [f"nw-{source}-job-run-*"]
    if include_unmapped:
        indices.append("nw-unmapped-job-run-*")
    return ",".join(indices)


def response_body(response):
    return response.body if hasattr(response, "body") else response


class CurrentReadError(RuntimeError):
    pass


class CurrentSnapshot:
    def __init__(self, client, config: NetworkerCurrentConfig, source):
        self.client, self.config = client, config
        self.source = source
        self.indices = job_indices(source, config.include_unmapped)
        self.pit = None
        self.count = 0

    async def __aenter__(self):
        caps = response_body(await self.client.field_caps(index=self.indices, fields=FIELDS, include_unmapped=True))
        for name in FIELDS:
            types = caps.get("fields", {}).get(name, {})
            expected = {"date", "date_nanos"} if name == "payload.end_time" else {"keyword"}
            if not types or any(t not in expected or v.get("searchable") is not True or v.get("non_searchable_indices") for t, v in types.items()):
                raise CurrentReadError("job_run_mapping_not_searchable")
        opened = response_body(await self.client.open_point_in_time(index=self.indices, keep_alive="2m", allow_partial_search_results=False))
        self.pit = opened.get("id")
        if not self.pit:
            raise CurrentReadError("missing_pit")
        if opened.get("_shards", {}).get("failed", 0):
            await self.__aexit__(None, None, None)
            raise CurrentReadError("partial_pit")
        return self

    async def __aexit__(self, *args):
        if self.pit:
            closed = response_body(await self.client.close_point_in_time(id=self.pit))
            if closed.get("succeeded") is not True:
                raise CurrentReadError("pit_close_failed")

    async def read(self, query):
        out, seen, after, total = [], set(), None, None
        while True:
            args = dict(pit={"id": self.pit, "keep_alive": "2m"}, query=query,
                        sort=["_shard_doc"], size=self.config.page_size,
                        track_total_hits=total is None, allow_partial_search_results=False)
            if after is not None:
                args["search_after"] = after
            response = response_body(await self.client.search(**args))
            self.pit = response.get("pit_id", self.pit)
            if response.get("timed_out") or response.get("terminated_early") or response.get("_shards", {}).get("failed", 0):
                raise CurrentReadError("partial_search")
            if total is None:
                t = response.get("hits", {}).get("total", {})
                if t.get("relation") != "eq" or type(t.get("value")) is not int or t["value"] < 0:
                    raise CurrentReadError("exact_total_required")
                total = t["value"]
            hits = response.get("hits", {}).get("hits")
            if not isinstance(hits, list):
                raise CurrentReadError("invalid_hits")
            if not hits:
                if len(out) != total:
                    raise CurrentReadError("total_mismatch")
                return out
            for hit in hits:
                key, cursor = (hit.get("_index"), hit.get("_id")), hit.get("sort")
                if (key in seen or not isinstance(hit.get("_source"), dict)
                        or not re.fullmatch(r"nw-(" + self.source + ("|unmapped" if self.config.include_unmapped else "") + r")-job-run-\d{4}-\d{2}", str(key[0]))
                        or not isinstance(cursor, list) or len(cursor) != 1 or type(cursor[0]) is not int
                        or after is not None and cursor[0] <= after[0]):
                    raise CurrentReadError("invalid_cursor_or_hit")
                seen.add(key)
                after = cursor
                out.append(hit)
                self.count += 1
                if self.count > self.config.max_hits:
                    raise CurrentReadError("snapshot_budget_exhausted")


def source_filter(source):
    return [{"term": {"source_networker": source}}, {"term": {"document_type": "job-run"}}]


async def read_jobs(client, config, source, days, incoming):
    earliest = window(min(days))[0].isoformat()
    query = {"bool": {"filter": source_filter(source) + [{"term": {"payload.type": "workflow job"}}],
                      "should": [{"range": {"payload.end_time": {"gte": earliest}}},
                                 {"bool": {"must_not": [{"exists": {"field": "payload.end_time"}}]}}],
                      "minimum_should_match": 1}}
    async with CurrentSnapshot(client, config, source) as snapshot:
        workflows = await snapshot.read(query)
        ids = sorted({str(h["_source"]["payload"]["job_id"]) for h in workflows + incoming
                      if h["_source"].get("source_networker") == source
                      and h["_source"]["payload"].get("type", "").lower() == "workflow job"})
        children = []
        for offset in range(0, len(ids), 200):
            chunk = ids[offset:offset + 200]
            # No child completion window: an earlier-finishing child remains evidence.
            children.extend(await snapshot.read({"bool": {"filter": source_filter(source),
                "should": [{"terms": {"payload.root_parent_job_id": chunk}},
                           {"terms": {"payload.parent_job_id": chunk}}], "minimum_should_match": 1}}))
    hits = {(h["_index"], h["_id"]): h for h in workflows + children}
    # Realtime inputs supplement an ES refresh delay; terminal copies are never regressed.
    for h in incoming:
        key = (h["_index"], h["_id"])
        old = hits.get(key)
        if old:
            op, np = old["_source"]["payload"], h["_source"]["payload"]
            if timestamp(op.get("end_time")) and not timestamp(np.get("end_time")):
                continue
            if op.get("start_time") != np.get("start_time"):
                hits[(key[0], key[1] + ":conflict")] = h
                continue
        hits[key] = h
    for h in hits.values():
        d = h["_source"]
        if d.get("source_networker") != source or d["payload"].get("source_networker", source) != source:
            raise CurrentReadError("source_identity_conflict")
    return list(hits.values())


async def hydrate_messages(client, hits, source):
    groups = defaultdict(set)
    pending = []
    for h in hits:
        doc, p = h["_source"], h["_source"]["payload"]
        if p.get("type", "").lower() != "workflow job" or p.get("message"):
            continue
        raw_id = doc.get("source_raw_id", "")
        match = re.search(r"(\d{4})(\d{2})\d{2}T\d{6}", raw_id)
        if match:
            index = f"nw-ops-raw-{source}-{match[1]}-{match[2]}"
            groups[index].add(raw_id)
            pending.append((h, index, raw_id))
    raw_jobs = {}
    for index, ids in groups.items():
        ordered = sorted(ids)
        for offset in range(0, len(ordered), 100):
            try:
                response = response_body(await client.mget(index=index, ids=ordered[offset:offset + 100],
                    source_includes=["collector", "ok", "payload.summary.source_networker", "payload.raw.jobs", "payload.collection_status"]))
            except NotFoundError:
                continue  # RAW retention loss does not erase a stored Workflow outcome.
            for hit in response.get("docs", []):
                d = hit.get("_source", {})
                payload = d.get("payload", {})
                if (hit.get("found") is not True or d.get("ok") is not True
                        or payload.get("summary", {}).get("source_networker") != source
                        or payload.get("collection_status") != "success"):
                    continue
                parsed = parse_networker_rest_payload(payload.get("raw", {}), d.get("collector", ""), source_networker=source)
                raw_jobs[(index, hit["_id"])] = {str(j["job_id"]): j for j in parsed["jobs"]}
    for hit, index, raw_id in pending:
        p = hit["_source"]["payload"]
        j = raw_jobs.get((index, raw_id), {}).get(str(p.get("job_id")), {})
        if (timestamp(j.get("start_time")) == timestamp(p.get("start_time"))
                and timestamp(j.get("end_time")) == timestamp(p.get("end_time"))
                and j.get("completion_status") == p.get("completion_status")):
            # In-memory enrichment only; original Job Run/RAW documents are untouched.
            p["message"] = j.get("message", "")


class NetworkerDailyCurrent:
    def __init__(self, client, bulk, config):
        self.client, self.bulk, self.config = client, bulk, config

    async def existing(self, source, day):
        try:
            return response_body(await self.client.get(index=f"nw-current-{source}", id=f"daily:{source}:{day}"))
        except NotFoundError:
            return None

    async def preserve_summary(self, source, day, summary):
        """Create an immutable generation summary in the SAME current index."""
        generation = summary.get("generation")
        if not generation:
            return
        index = f"nw-current-{source}"
        identity = f"daily:{source}:{day}:{generation}"

        async def matching_existing():
            try:
                existing = response_body(await self.client.get(index=index, id=identity))["_source"]
            except NotFoundError:
                return False
            if (existing.get("generation") != generation or existing.get("source_networker") != source
                    or existing.get("backup_day") != day or existing.get("summary_role") != "generation"
                    or existing.get("detail_ids") != summary.get("detail_ids")):
                raise CurrentReadError("generation_summary_identity_conflict")
            return True

        if await matching_existing():
            return
        action = {"_op_type": "create", "_index": index, "_id": identity,
                  "_source": dict(summary, summary_role="generation")}
        try:
            await self.send([action])
        except (ConflictError, BulkIndexError) as exc:
            # A concurrent identical publication can create the snapshot first.
            # Other bulk errors must not be treated as an idempotent conflict.
            if isinstance(exc, BulkIndexError) and (not exc.errors or any(
                    item.get("create", {}).get("status") != 409
                    or item.get("create", {}).get("_id") != identity for item in exc.errors)):
                raise
            if not await matching_existing():
                raise

    async def check_previous(self, source, previous, hits):
        """Use published results only as a regression guard, never as invented source rows."""
        by_id = defaultdict(list)
        for h in hits:
            p = h["_source"]["payload"]
            if p.get("type", "").lower() == "workflow job":
                by_id[str(p.get("job_id"))].append(h["_source"])
        ids = sorted({i for old in previous.values() if old for i in old["_source"].get("detail_ids", [])})
        for offset in range(0, len(ids), 100):
            response = response_body(await self.client.mget(index=f"nw-current-{source}", ids=ids[offset:offset + 100]))
            docs = response.get("docs", [])
            if len(docs) != len(ids[offset:offset + 100]) or any(d.get("found") is not True for d in docs):
                raise CurrentReadError("previous_generation_details_missing")
            for d in docs:
                row = d["_source"]
                if row.get("dashboard_result") not in {"success", "failed"}:
                    continue
                copies = by_id.get(row["workflow_job_id"], [])
                if not copies:
                    raise CurrentReadError("previous_source_execution_missing")
                terminal = [c for c in copies if timestamp(c["payload"].get("end_time"))
                            or str(c["payload"].get("completion_status") or "").lower() in RESULTS
                            or str(c["payload"].get("state_raw") or c["payload"].get("state") or "").lower() in RESULTS]
                if not terminal:
                    raise CurrentReadError("terminal_observation_regression")
                observed = timestamp(row.get("source_observed_at"))
                if observed and all(not timestamp(c.get("observed_at")) or timestamp(c["observed_at"]) < observed for c in terminal):
                    raise CurrentReadError("older_source_observation")

    async def publish(self, source, incoming, now, collection_complete):
        if source not in {"core", "chnl", "info"}:
            raise ValueError("invalid current source")
        days = closed_days(now, source, self.config)
        # Read publication versions BEFORE the input snapshot, not after computing it.
        previous = {d: await self.existing(source, d) for d in days}
        try:
            hits = await read_jobs(self.client, self.config, source, days, incoming)
            extra_days = discovered_days(hits, source, self.config, max(days)) - set(days)
            if extra_days:
                # Preserve the existing CAS contract: read all new day versions
                # BEFORE opening the complete, expanded input snapshot again.
                previous.update({d: await self.existing(source, d) for d in sorted(extra_days)})
                days = sorted(set(days) | extra_days)
                hits = await read_jobs(self.client, self.config, source, days, incoming)
            await self.check_previous(source, previous, hits)
            await hydrate_messages(self.client, hits, source)
            reasons = [] if collection_complete else ["latest_jobs_collection_partial"]
            computed = build_daily(hits, source, days, now, self.config, input_reasons=reasons)
        except Exception as exc:
            # Preserve every previous result/generation; publish only availability metadata.
            for day in days:
                old = previous[day]
                reason = str(exc) if isinstance(exc, CurrentReadError) else "current_input_read_failed"
                update = {"summary_role": "head", "data_status": "unavailable", "aggregation_status": "unavailable",
                          "data_reasons": [reason], "attempted_at": now.isoformat()}
                if old:
                    action = {"_op_type": "update", "_index": f"nw-current-{source}", "_id": f"daily:{source}:{day}",
                              "if_seq_no": old["_seq_no"], "if_primary_term": old["_primary_term"], "doc": update}
                else:
                    start, end = window(day)
                    action = {"_op_type": "create", "_index": f"nw-current-{source}", "_id": f"daily:{source}:{day}",
                              "_source": update | {"document_kind": "daily_summary", "source_networker": source, "backup_day": day,
                                  "window_start": start.isoformat(), "window_end": end.isoformat(), "generation": None,
                                  "success_count": None, "failure_count": None, "success_rate": None}}
                await self.send([action])
            raise
        for day, (summary, rows) in computed.items():
            index = f"nw-current-{source}"
            old = previous[day]
            if old:
                # Upgrade an existing fixed-ID-only head BEFORE it can be replaced.
                # Existing snapshots are never overwritten by availability updates.
                await self.preserve_summary(source, day, old["_source"])
            actions = [{"_op_type": "index", "_index": index,
                        "_id": f"workflow:{source}:{day}:{digest([r['policy_name'], r['workflow_name'], r['workflow_job_id'] if not r['policy_name'] or not r['workflow_name'] or r['attribution'] == 'unresolved' else None])[:24]}:{summary['generation']}",
                        "_source": r} for r in rows]
            summary["detail_ids"] = [a["_id"] for a in actions]
            if actions:
                await self.send(actions)  # Only switch the summary after all details are visible.
            await self.preserve_summary(source, day, summary)
            action = {"_op_type": "index" if old else "create", "_index": index, "_id": f"daily:{source}:{day}",
                      "_source": dict(summary, summary_role="head")}
            if old:
                action.update(if_seq_no=old["_seq_no"], if_primary_term=old["_primary_term"])
            await self.send([action])

    async def send(self, actions):
        result = await self.bulk(self.client, actions, refresh="wait_for", raise_on_error=True,
                                 chunk_size=250, max_chunk_bytes=10 * 1024 * 1024)
        if isinstance(result, tuple) and result[1]:
            raise CurrentReadError("current_bulk_errors")
