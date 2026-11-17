"""Read complete ES snapshots and publish daily current through the existing writer."""
from __future__ import annotations

import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime
from functools import partial
import logging
import re
import time

from elasticsearch import ConflictError, NotFoundError
from elasticsearch.helpers import BulkIndexError

from apps.core.config import NetworkerCurrentConfig
from apps.parsers.networker_rest_parser import parse_networker_rest_payload
from apps.processors.networker_current import (ACTIVE, RESULTS, InventoryTargets, build_daily, closed_days, daily_failure_view,
    discovered_days, digest, running_candidates, timestamp, window)

FIELDS = ["source_networker", "document_type", "payload.type", "payload.end_time", "payload.parent_job_id", "payload.root_parent_job_id"]
LOGGER = logging.getLogger(__name__)


@asynccontextmanager
async def _current_stage(source, name, progress):
    """One summary per phase; never log document contents or exception messages."""
    progress["stage"] = name
    counts, status, error_type = {}, "completed", None
    started = time.perf_counter()
    try:
        yield counts
    except asyncio.CancelledError:
        status = "cancelled"
        raise
    except Exception as exc:
        status, error_type = "failed", type(exc).__name__
        raise
    finally:
        LOGGER.log(logging.INFO if status == "completed" else logging.WARNING,
                   "NetWorker current source=%s stage=%s status=%s elapsed_seconds=%.3f counts=%s error_type=%s",
                   source, name, status, time.perf_counter() - started, counts, error_type)


async def _compute_daily(hits, source, days, now, config, reasons, inventory=None):
    return await _compute_cpu(partial(build_daily, hits, source, days, now, config, input_reasons=reasons, inventory=inventory))


async def _compute_cpu(function):
    # Existing Writer source locks bound this to one computation per source.
    # Keep this await inside that lock, including cancellation/shutdown cleanup.
    worker = asyncio.get_running_loop().run_in_executor(None, function)
    try:
        result = await asyncio.shield(worker)
        # Deliver pending caller cancellation before the first publication.
        await asyncio.sleep(0)
        return result
    except asyncio.CancelledError:
        # Cancelling an await does not stop a running Python thread. Drain it
        # before propagating cancellation and releasing the caller's source lock.
        # Repeated cancellation must not interrupt this cleanup either.
        while not worker.done():
            try:
                await asyncio.shield(worker)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        if not worker.cancelled():
            worker.exception()  # Consume a discarded failure; never publish its result.
        raise


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
        self.pages = 0

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
            self.pages += 1  # Includes terminal empty responses, not extra queries.
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


async def read_jobs(client, config, source, days, incoming, *, stats=None):
    """Read unchanged queries; optional counters include duplicate hits and rescans."""
    earliest = window(min(days))[0].isoformat()
    query = {"bool": {"filter": source_filter(source) + [{"term": {"payload.type": "workflow job"}}],
                      "should": [{"range": {"payload.end_time": {"gte": earliest}}},
                                 {"bool": {"must_not": [{"exists": {"field": "payload.end_time"}}]}}],
                      "minimum_should_match": 1}}
    async with CurrentSnapshot(client, config, source) as snapshot:
        reading_workflows, workflow_hits = True, 0
        try:
            workflows = await snapshot.read(query)
            reading_workflows, workflow_hits = False, len(workflows)
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
        finally:
            if stats is not None:
                workflow_hits = snapshot.count if reading_workflows else workflow_hits
                for name, count in (("workflow_hits", workflow_hits), ("child_hits", snapshot.count - workflow_hits),
                                    ("page_responses", snapshot.pages), ("snapshot_reads", 1)):
                    stats[name] = stats.get(name, 0) + count
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
    if stats is not None:
        stats["merged_hits"] = len(hits)
    return list(hits.values())


async def read_inventory(client, config, source, now, *, stats=None):
    """Read existing RAW Inventory snapshots, newest first. Never query disabled payload fields.

    Stop after the latest successful complete Inventory and equal-time copies. A failed
    attempt is retained as availability evidence, while the last good input survives.
    """
    index = f"nw-ops-raw-{source}-*"
    documents, seen, after, pit, reason, boundary = [], set(), None, None, None, None
    try:
        caps = response_body(await client.field_caps(index=index, fields=["@timestamp", "collection_class"], include_unmapped=True))
        for name in ("@timestamp", "collection_class"):
            values = caps.get("fields", {}).get(name, {})
            if not values or any(v.get("searchable") is not True for v in values.values()):
                raise CurrentReadError("inventory_metadata_not_searchable")
        opened = response_body(await client.open_point_in_time(index=index, keep_alive="2m", allow_partial_search_results=False))
        pit = opened.get("id")
        if not pit or opened.get("_shards", {}).get("failed", 0):
            raise CurrentReadError("inventory_partial_pit")
        while True:
            args = dict(pit={"id": pit, "keep_alive": "2m"},
                query={"bool": {"filter": [{"terms": {"collection_class": ["inventory", "slow"]}},
                                            {"range": {"@timestamp": {"lte": now.isoformat()}}}]}},
                sort=[{"@timestamp": "desc"}, "_shard_doc"], size=min(config.page_size, 8),
                track_total_hits=False, allow_partial_search_results=False,
                source_includes=["@timestamp", "ok", "collection_class", "payload.summary.source_networker",
                    "payload.collection_status", "payload.endpoint_errors", "payload.inventory_status",
                    "payload.clients", "payload.workflows", "payload.protection_groups"])
            if after is not None:
                args["search_after"] = after
            response = response_body(await client.search(**args))
            pit = response.get("pit_id", pit)
            if stats is not None:
                stats["pages"] = stats.get("pages", 0) + 1
            if response.get("timed_out") or response.get("terminated_early") or response.get("_shards", {}).get("failed", 0):
                raise CurrentReadError("inventory_partial_search")
            page = response.get("hits", {}).get("hits")
            if not isinstance(page, list):
                raise CurrentReadError("inventory_invalid_hits")
            if not page:
                break
            finished = False
            for hit in page:
                key, cursor, d = (hit.get("_index"), hit.get("_id")), hit.get("sort"), hit.get("_source")
                if (key in seen or not re.fullmatch(r"nw-ops-raw-" + source + r"-\d{4}-\d{2}", str(key[0]))
                        or not isinstance(d, dict) or not isinstance(cursor, list) or len(cursor) != 2
                        or after is not None and (cursor[0] > after[0] or cursor[0] == after[0] and cursor[1] <= after[1])):
                    raise CurrentReadError("inventory_invalid_cursor_or_hit")
                seen.add(key)
                after = cursor
                if len(seen) > config.max_hits:
                    raise CurrentReadError("inventory_read_budget_exhausted")
                observed = timestamp(d.get("@timestamp"))
                p = d.get("payload", {})
                if boundary and observed and observed < boundary:
                    finished = True
                    break
                if not observed or observed > now or p.get("summary", {}).get("source_networker") != source:
                    raise CurrentReadError("inventory_source_or_time_conflict")
                documents.append(hit)
                if InventoryTargets.complete_document(d, source, now):
                    if boundary is None:
                        boundary = observed
            # Read THROUGH the selected timestamp, including ties across pages.
            if finished:
                break
    except Exception as exc:
        documents = []  # A broken snapshot cannot publish partially read relationships.
        reason = str(exc) if isinstance(exc, CurrentReadError) else "inventory_read_unavailable"
    finally:
        if pit:
            try:
                closed = response_body(await client.close_point_in_time(id=pit))
                if closed.get("succeeded") is not True:
                    raise CurrentReadError("inventory_pit_close_failed")
            except Exception:
                documents, reason = [], "inventory_pit_close_failed"
        if stats is not None:
            stats.update(snapshot_hits=len(documents), read_status="unavailable" if reason else "complete")
    return await _compute_cpu(partial(InventoryTargets, documents, source, now, reason))


async def hydrate_messages(client, hits, source, *, stats=None):
    if stats is not None:
        stats.update(requested_raw_documents=0, returned_raw_documents=0, processed_jobs=0, enriched_workflows=0)
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
        if stats is not None:
            stats["requested_raw_documents"] += len(ordered)
        for offset in range(0, len(ordered), 100):
            try:
                response = response_body(await client.mget(index=index, ids=ordered[offset:offset + 100],
                    source_includes=["collector", "ok", "payload.summary.source_networker", "payload.raw.jobs", "payload.collection_status"]))
            except NotFoundError:
                continue  # RAW retention loss does not erase a stored Workflow outcome.
            for hit in response.get("docs", []):
                if stats is not None and hit.get("found") is True:
                    stats["returned_raw_documents"] += 1
                d = hit.get("_source", {})
                payload = d.get("payload", {})
                if (hit.get("found") is not True or d.get("ok") is not True
                        or payload.get("summary", {}).get("source_networker") != source
                        or payload.get("collection_status") != "success"):
                    continue
                parsed = parse_networker_rest_payload(payload.get("raw", {}), d.get("collector", ""), source_networker=source)
                if stats is not None:
                    stats["processed_jobs"] += len(parsed["jobs"])
                raw_jobs[(index, hit["_id"])] = {str(j["job_id"]): j for j in parsed["jobs"]}
    for hit, index, raw_id in pending:
        p = hit["_source"]["payload"]
        j = raw_jobs.get((index, raw_id), {}).get(str(p.get("job_id")), {})
        if (timestamp(j.get("start_time")) == timestamp(p.get("start_time"))
                and timestamp(j.get("end_time")) == timestamp(p.get("end_time"))
                and j.get("completion_status") == p.get("completion_status")):
            # In-memory enrichment only; original Job Run/RAW documents are untouched.
            p["message"] = j.get("message", "")
            if stats is not None and p["message"]:
                stats["enriched_workflows"] += 1


class NetworkerDailyCurrent:
    def __init__(self, client, bulk, config):
        self.client, self.bulk, self.config = client, bulk, config

    async def existing(self, source, day):
        try:
            return response_body(await self.client.get(index=f"nw-current-{source}", id=f"daily:{source}:{day}"))
        except NotFoundError:
            return None

    async def current_document(self, source, identity):
        try:
            result = response_body(await self.client.get(index=f"nw-current-{source}", id=identity))
            if result.get("_source", {}).get("source_networker") != source:
                raise CurrentReadError("current_source_identity_conflict")
            return result
        except NotFoundError:
            return None

    def versioned(self, source, identity, document, old):
        action = {"_op_type": "index" if old else "create", "_index": f"nw-current-{source}",
                  "_id": identity, "_source": document}
        if old:
            action.update(if_seq_no=old["_seq_no"], if_primary_term=old["_primary_term"])
        return action

    async def publish_running(self, source, hits, incoming, now, observation, inventory=None):
        """Fixed execution IDs plus a small unfinished ledger; no historical ES scan."""
        ledger_id = f"running:{source}"
        previous = await self.current_document(source, ledger_id)
        unfinished = dict(previous["_source"].get("unfinished_workflows", {})) if previous else {}
        jobs = await _compute_cpu(partial(running_candidates, hits, source, inventory))
        requested = set(unfinished)
        requested.update(str(h["_source"]["payload"].get("job_id")) for h in incoming
                         if h["_source"].get("source_networker") == source
                         and h["_source"]["payload"].get("type", "").lower() == "workflow job"
                         and not timestamp(h["_source"]["payload"].get("end_time"))
                         and str(h["_source"]["payload"].get("completion_status") or "").lower() not in RESULTS)
        requested.update(i for i, j in jobs.items() if not timestamp(j.get("end_time"))
                         and str(j.get("state_raw") or j.get("state") or "").lower() in ACTIVE)
        observed = set((observation or {}).get("observed_workflow_ids", []))
        actions, running_count, unknown_count = [], 0, 0
        for identity in sorted(requested):
            fixed_id = f"running:{source}:{identity}"
            old = await self.current_document(source, fixed_id)
            retained = old["_source"] if old else {}
            j = jobs.get(identity)
            if j is None and not retained:
                # Keep a corrupt/missing tracked row visible as an unresolved ID,
                # never silently clear the unfinished ledger.
                j = {"job_id": identity, "start_time": unfinished.get(identity)}
            completion = str((j or {}).get("completion_status") or "").lower()
            state = str((j or {}).get("state_raw") or (j or {}).get("state") or "").lower()
            terminal = bool(j and (timestamp(j.get("end_time")) or completion in RESULTS or state in RESULTS))
            newer = bool(j and (not timestamp(retained.get("last_observed_at")) or
                         timestamp(j.get("source_observed_at")) and
                         timestamp(j["source_observed_at"]) >= timestamp(retained["last_observed_at"])))
            row = dict(retained)
            observed_fields = ({"policy_name": j.get("policy_name") or j.get("name") or None,
                "workflow_name": j.get("workflow_name") or None, "start_time": j.get("start_time"),
                "end_time": j.get("end_time"), "source_state": j.get("state_raw") or j.get("state"),
                "source_completion_status": j.get("completion_status"),
                "last_observed_at": j.get("source_observed_at"),
                **{k: j.get(k) for k in ("hostname", "hostnames", "hostname_status", "hostname_sources",
                    "configured_hostnames", "observed_hostnames", "hostname_relation_status", "inventory_observed_at",
                    "inventory_last_attempt_at", "inventory_temporal_basis", "inventory_status", "inventory_reasons",
                    "inventory_id", "inventory_last_collection_status",
                    "observed_hostname_conflict")}} if j else {})
            if not row:
                row.update(observed_fields)
            conflict = bool(j and retained.get("start_time") and
                            timestamp(j.get("start_time")) != timestamp(retained["start_time"]))
            start, end = timestamp((j or {}).get("start_time")), timestamp((j or {}).get("end_time"))
            invalid_times = bool(start and start > now or end and end > now or start and end and end < start)
            if conflict or invalid_times or j and j.get("relationship_issues"):
                row.update(execution_status="unconfirmed", is_running=False,
                           state_reason="invalid_execution_times" if invalid_times else "job_identity_or_terminal_conflict")
                unfinished[identity] = retained.get("start_time") or j.get("start_time")
            elif retained.get("execution_status") == "completed" and (not terminal or not newer):
                unfinished.pop(identity, None)  # A stale active observation cannot reopen a terminal execution.
            elif j and (terminal or newer):
                row.update(observed_fields)
                confirmed = identity in observed
                active = not terminal and (state in ACTIVE or completion in ACTIVE) and confirmed
                row.update(execution_status="completed" if terminal else "running" if active else "unconfirmed",
                           is_running=active, state_reason=None if terminal or active else "completion_not_confirmed")
                if terminal:
                    unfinished.pop(identity, None)
                else:
                    unfinished[identity] = j.get("start_time")
            elif retained.get("execution_status") != "completed":
                row.update(execution_status="unconfirmed", is_running=False, state_reason="completion_not_confirmed")
            row.update(document_kind="workflow_execution", source_networker=source, workflow_job_id=identity,
                       updated_at=now.isoformat(), last_attempt_at=now.isoformat())
            if row.get("execution_status") == "running":
                running_count += 1
            if row.get("execution_status") == "unconfirmed":
                unknown_count += 1
            actions.append(self.versioned(source, fixed_id, row, old))
        # Mark availability BEFORE writing execution rows. On partial bulk/crash,
        # callers can see unavailable status instead of interpreting a false zero.
        status = {"document_kind": "running_status", "source_networker": source,
                  "updated_at": now.isoformat(), "last_attempt_at": now.isoformat(),
                  "data_status": "updating", "running_count": None,
                  "unconfirmed_workflow_count": None,
                  "unfinished_workflows": ((previous or {}).get("_source", {}).get("unfinished_workflows", {}) | unfinished)}
        status["generation"] = digest([source, now.isoformat(), [a["_source"] for a in actions], unfinished])[:24]
        await self.send([self.versioned(source, ledger_id, status, previous)])
        version = await self.current_document(source, ledger_id)
        if not version or version["_source"].get("generation") != status["generation"]:
            raise CurrentReadError("running_publication_superseded")
        if actions:
            await self.send(actions)
        success = (observation or {}).get("active_success") is True
        status.update(data_status="complete" if success and not unknown_count else "partial" if success else "unavailable",
                      running_count=running_count if success else None,
                      unconfirmed_workflow_count=unknown_count,
                      unfinished_workflows=unfinished,
                      collection_observed_at=now.isoformat() if success else (previous or {}).get("_source", {}).get("collection_observed_at"))
        await self.send([self.versioned(source, ledger_id, status, version)])
        return {"tracked_workflows": len(requested), "confirmed_running": running_count,
                "unconfirmed_workflows": unknown_count, "active_query_success": success}

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

    async def publish(self, source, incoming, now, collection_complete, *, observation=None):
        started = time.perf_counter()
        progress = {"stage": "initialize"}
        status, error_type = "completed", None
        try:
            await self._publish(source, incoming, now, collection_complete, progress, observation)
        except asyncio.CancelledError:
            status = "cancelled"
            raise
        except Exception as exc:
            status, error_type = "failed", type(exc).__name__
            raise
        finally:
            LOGGER.log(logging.INFO if status == "completed" else logging.WARNING,
                       "NetWorker current source=%s stage=total status=%s last_stage=%s elapsed_seconds=%.3f error_type=%s",
                       source, status, progress["stage"], time.perf_counter() - started, error_type)

    async def _publish(self, source, incoming, now, collection_complete, progress, observation):
        if source not in {"core", "chnl", "info"}:
            raise ValueError("invalid current source")
        async with _current_stage(source, "existing_heads", progress) as counts:
            days = closed_days(now, source, self.config)
            counts["target_days"] = len(days)
            # Read publication versions BEFORE the input snapshot, not after computing it.
            previous = {d: await self.existing(source, d) for d in days}
        try:
            async with _current_stage(source, "job_read", progress) as counts:
                counts.update(workflow_hits=0, child_hits=0, page_responses=0, snapshot_reads=0)
                hits = await read_jobs(self.client, self.config, source, days, incoming, stats=counts)
                extra_days = discovered_days(hits, source, self.config, max(days)) - set(days)
                if extra_days:
                    # Preserve the existing CAS contract: read all new day versions
                    # BEFORE opening the complete, expanded input snapshot again.
                    previous.update({d: await self.existing(source, d) for d in sorted(extra_days)})
                    days = sorted(set(days) | extra_days)
                    hits = await read_jobs(self.client, self.config, source, days, incoming, stats=counts)
            async with _current_stage(source, "previous_check", progress):
                await self.check_previous(source, previous, hits)
            async with _current_stage(source, "raw_enrichment", progress) as counts:
                await hydrate_messages(self.client, hits, source, stats=counts)
            async with _current_stage(source, "inventory_read", progress) as counts:
                inventory = await read_inventory(self.client, self.config, source, now, stats=counts)
            async with _current_stage(source, "aggregate", progress) as counts:
                counts.update(input_hits=len(hits), target_days=len(days), workflow_executions=len({
                    str(h["_source"]["payload"]["job_id"]) for h in hits
                    if h["_source"].get("source_networker") == source
                    and h["_source"]["payload"].get("type", "").lower() == "workflow job"
                    and h["_source"]["payload"].get("job_id") is not None}))
                reasons = [] if collection_complete else ["latest_jobs_collection_partial"]
                computed = await _compute_daily(hits, source, days, now, self.config, reasons, inventory)
            async with _current_stage(source, "running", progress) as counts:
                counts.update(await self.publish_running(source, hits, incoming, now, observation, inventory))
        except Exception as exc:
            # Preserve every previous result/generation; publish only availability metadata.
            old_running = await self.current_document(source, f"running:{source}")
            if old_running and (not timestamp(old_running["_source"].get("last_attempt_at"))
                                or timestamp(old_running["_source"]["last_attempt_at"]) <= now):
                unavailable = dict(old_running["_source"], data_status="unavailable", running_count=None,
                                   last_attempt_at=now.isoformat())
                await self.send([self.versioned(source, f"running:{source}", unavailable, old_running)])
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
                old_projection = await self.current_document(source, f"policy-host:{source}:{day}")
                if old_projection and (not timestamp(old_projection["_source"].get("updated_at"))
                                       or timestamp(old_projection["_source"]["updated_at"]) <= now):
                    projection = dict(old_projection["_source"], projection_status="unavailable",
                                      updated_at=now.isoformat(), failed_policy_host_keys=[])
                    await self.send([self.versioned(source, f"policy-host:{source}:{day}", projection, old_projection)])
            raise
        async with _current_stage(source, "publish", progress) as counts:
            counts.update(target_days=len(computed), published_days=0)
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
                projection_id = f"policy-host:{source}:{day}"
                old_projection = await self.current_document(source, projection_id)
                projection = await _compute_cpu(partial(daily_failure_view, summary, rows))
                # Hide the old failure set before changing the head. A crash/CAS
                # failure leaves explicit unavailability, never an unposted generation.
                await self.send([self.versioned(source, projection_id,
                    dict(projection, projection_status="updating", failed_policy_host_keys=[]), old_projection)])
                projection_version = await self.current_document(source, projection_id)
                if not projection_version or projection_version["_source"].get("generation") != summary["generation"]:
                    raise CurrentReadError("daily_projection_superseded")
                action = {"_op_type": "index" if old else "create", "_index": index, "_id": f"daily:{source}:{day}",
                          "_source": dict(summary, summary_role="head")}
                if old:
                    action.update(if_seq_no=old["_seq_no"], if_primary_term=old["_primary_term"])
                await self.send([action])
                if not collection_complete:
                    projection.update(projection_status="unavailable", failed_policy_host_keys=[])
                await self.send([self.versioned(source, projection_id, projection, projection_version)])
                counts["published_days"] += 1

    async def send(self, actions):
        result = await self.bulk(self.client, actions, refresh="wait_for", raise_on_error=True,
                                 chunk_size=250, max_chunk_bytes=10 * 1024 * 1024)
        if isinstance(result, tuple) and result[1]:
            raise CurrentReadError("current_bulk_errors")
