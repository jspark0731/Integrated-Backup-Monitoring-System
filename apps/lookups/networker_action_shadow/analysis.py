"""Aggregate-only validation and orchestration for manual shadow runs."""

from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone

from apps.lookups.networker_action.aggregate import ActionLookupBuilder
from .reader import PITReader, range_query, validate_indices
from apps.lookups.networker_action.extract import extract_raw
from apps.lookups.networker_action.models import source_of, timestamp
from apps.lookups.networker_action.validate import ACTION_TYPE, read_job, select_job, validate_save

BASELINE = {"save job": 399, "workflow job": 9, "backup action job": 5,
            "savefs job": 5, "recover pipe to save job": 7}
JOB_KINDS = tuple(BASELINE) + ("other",)


def analyse(raw, jobs, source, *, synced_at=None, generation="dry-run"):
    builder = ActionLookupBuilder()
    for hit in jobs:
        if source_of(hit["_source"]) != source:
            raise ValueError("input_source_mismatch")
    for hit in raw:
        if source_of(hit["_source"], raw=True) != source:
            raise ValueError("input_source_mismatch")
    builder.add_jobs(jobs)
    builder.add_raw(raw)
    documents = builder.build(synced_at=synced_at or datetime.now(timezone.utc), generation=generation,
                              reference_limit=1000)
    kinds, statuses = Counter(), Counter()
    for hit in jobs:
        current = read_job(hit)
        kind = current.kind
        kinds[kind if kind in JOB_KINDS else "other"] += 1
        statuses[current.status if current.status in {"success", "failed", "running", "canceled", "unknown"} else "unknown"] += 1
    hierarchy = Counter()
    selected = []
    for copies in builder.jobs.values():
        job, conflicts = select_job(copies)
        selected.append(job)
        if conflicts:
            hierarchy["logical_job_conflicts"] += 1
        hierarchy["repeated_job_observations"] += len(copies) - 1
    for job in selected:
        if job.kind == "save job":
            hierarchy["save_jobs_logical"] += 1
            if not job.parent:
                hierarchy["save_parent_zero_or_invalid"] += 1
                continue
            parent, conflicts = select_job(builder.jobs.get((job.source, job.parent), []))
            if parent is None:
                hierarchy["save_parent_missing"] += 1
            elif parent.kind != ACTION_TYPE:
                hierarchy["save_parent_other_type"] += 1
            else:
                hierarchy["save_to_action_linked"] += 1
                save_conflicts = validate_save(job, parent)
                if not conflicts and not save_conflicts:
                    hierarchy["save_to_action_validated"] += 1
                else:
                    hierarchy["save_action_conflict"] += 1
                for name in ["save_root_conflict", "save_time_conflict", "save_parent_mismatch"]:
                    hierarchy[name] += name in save_conflicts
        elif job.kind == ACTION_TYPE:
            hierarchy["action_jobs_logical"] += 1
            if not job.root:
                hierarchy["action_root_zero_or_invalid"] += 1
            else:
                workflow, conflicts = select_job(builder.jobs.get((job.source, job.root), []))
                if workflow is None:
                    hierarchy["action_workflow_missing"] += 1
                elif workflow.kind != "workflow job":
                    hierarchy["action_root_other_type"] += 1
                elif conflicts:
                    hierarchy["action_workflow_conflict"] += 1
                else:
                    hierarchy["action_to_workflow_linked"] += 1
    reasons = Counter(r for d in documents.values() for r in d["reason"].split(";") if r)
    raw_items = sum(len(extract_raw(h).evidence) for h in raw)
    windows, collection_failures, backup_items = [], 0, 0
    for hit in raw:
        doc = hit["_source"]
        payload = doc.get("payload", {})
        collection_failures += bool(doc.get("ok") is False or payload.get("collection_status") in {"partial", "error"} or payload.get("endpoint_errors"))
        summary = payload.get("summary", {})
        container = payload.get("raw", {})
        backups = container.get("backups")
        backups = backups.get("backups") if isinstance(backups, dict) else backups
        if isinstance(backups, list):
            backup_items += len(backups)
        window = summary.get("query_window") or container.get("_query_window") or {}
        endpoint = window.get("endpoints", {}).get("backups", window)
        start, end = timestamp(endpoint.get("start")), timestamp(endpoint.get("end"))
        if start and end and start < end:
            windows.append((start, end))
    gaps, previous = 0, None
    for start, end in sorted(windows):
        if previous is not None and start > previous:
            gaps += 1
        previous = max(previous, end) if previous else end
    report = {
        "source_networker": source, "job_run_documents": len(jobs), "raw_documents": len(raw),
        "job_types": {k: kinds[k] for k in JOB_KINDS},
        "original_job_statuses": {k: statuses[k] for k in ["success", "failed", "running", "canceled", "unknown"]},
        "baseline_comparison": {"expected_total": 425, "actual_total": len(jobs), "total_matches": len(jobs) == 425,
            "types": {k: {"expected": v, "actual": kinds[k], "matches": kinds[k] == v} for k, v in BASELINE.items()},
            "save_action_expected": 61, "save_action_actual": hierarchy["save_to_action_linked"],
            "save_action_matches": hierarchy["save_to_action_linked"] == 61,
            "action_workflow_expected": 5, "action_workflow_actual": hierarchy["action_to_workflow_linked"],
            "action_workflow_matches": hierarchy["action_to_workflow_linked"] == 5,
            "fic_missing_expected": 323, "non_fic_missing_expected": 15,
            "client_subgroups_evaluated": False},
        "action_lookup_documents": len(documents), "candidate_tuples": sum(d["candidate_count"] for d in documents.values()),
        "mapping_status": {k: sum(d["mapping_status"] == k for d in documents.values()) for k in
                           ["resolved", "ambiguous", "unresolved", "invalid"]},
        "verification_status": {k: sum(d["verification_status"] == k for d in documents.values()) for k in
                                ["verified", "provisional", "unverified", "blocked"]},
        "conflict_documents": sum(d["mapping_status"] == "ambiguous" or d["verification_status"] == "blocked" for d in documents.values()),
        "missing_action_job": reasons["missing_action_job"], "workflow_job_missing": reasons["workflow_job_missing"],
        "hierarchy": dict(hierarchy),
        "direct_execution_candidates": hierarchy["save_parent_zero_or_invalid"],
        "direct_execution_confirmed": 0,
        "predeployment_parent_missing_possible": hierarchy["save_parent_missing"],
        "predeployment_parent_missing_confirmed": 0,
        "raw_evidence": {"valid_observations": raw_items,
            "backup_items": backup_items,
            "unique_backup_identities": len(builder.identities),
            "quarantined_issues": sum(len(extract_raw(h).issues) for h in raw),
            "partial_sources": len(builder.incomplete_sources), "api_coverage_proven": False,
            "collection_failure_documents": collection_failures,
            "explicit_api_window_documents": len(windows), "missing_or_ambiguous_api_window_documents": len(raw) - len(windows),
            "observed_internal_window_gaps": gaps},
        "incomplete": True,
        "builder_issue_count": len(builder.issues),
        "incomplete_reasons": ["api_coverage_not_attested", "suffix_rule_not_reviewed"],
    }
    if builder.incomplete_sources:
        report["incomplete_reasons"].append("raw_collection_partial")
    if report["missing_action_job"] or report["workflow_job_missing"]:
        report["incomplete_reasons"].append("relationship_references_missing")
    if builder.issues:
        report["incomplete_reasons"].append("input_quarantined_issues")
    return {"generation": generation, "documents": documents, "report": report}


async def execute(http, *, source, job_indices, raw_indices, job_start, job_end, raw_start, raw_end,
                  state=None, publisher=None, create_shadow=False, page_size=100, max_hits=100000):
    if (state is None) != (publisher is None):
        raise ValueError("shadow_state_and_publisher_required_together")
    for indices, channel in [(job_indices, "jobs"), (raw_indices, "raw")]:
        validate_indices(indices, channel)
    # Validate windows even when restoring a pending publication.
    queries = {"jobs": range_query(source, "jobs", job_start, job_end),
               "raw": range_query(source, "raw", raw_start, raw_end)}
    if state and (pending := state.pending()):
        await publisher.prepare(create=create_shadow)
        published = await state.publish(publisher, pending)
        return pending["report"] | {"mode": "shadow", "resumed_pending": True, "publisher": published}
    batches, audits = {}, {}
    for channel, indices, start, end in [("jobs", job_indices, job_start, job_end), ("raw", raw_indices, raw_start, raw_end)]:
        reader = PITReader(http, page_size=page_size, max_hits=max_hits)
        await reader.preflight(indices, channel)
        batches[channel] = await reader.read(indices, channel, queries[channel])
        for hit in batches[channel]:
            document = hit["_source"]
            observed = timestamp(document.get("observed_at" if channel == "jobs" else "@timestamp"))
            if observed is None or not timestamp(start) <= observed < timestamp(end):
                raise ValueError("out_of_window_response")
        audits[channel] = asdict(reader.audit)
    plan = analyse(batches["raw"], batches["jobs"], source)
    plan["report"]["reads"] = audits
    plan["report"]["read_coverage"] = {"stored_scope_complete": True, "api_coverage_proven": False,
        "job_observed_from": timestamp(job_start).isoformat(), "job_observed_to": timestamp(job_end).isoformat(),
        "raw_collected_from": timestamp(raw_start).isoformat(), "raw_collected_to": timestamp(raw_end).isoformat(),
        "cross_channel_atomic_snapshot": False}
    if state:
        current_report = plan["report"]

        def build_plan(raw, jobs, generation):
            built = analyse(raw, jobs, source, generation=generation)
            # Current fixed-window baseline and cumulative ledger are distinct.
            built["report"] = current_report | {"cumulative_ledger": built["report"]}
            return built

        plan = state.stage(batches["raw"], batches["jobs"], build_plan, max_records=max_hits)
        await publisher.prepare(create=create_shadow)
        result = await state.publish(publisher, plan)
        return current_report | {"mode": "shadow", "resumed_pending": False,
            "cumulative_ledger": plan["report"].get("cumulative_ledger", plan["report"]), "publisher": result}
    return plan["report"] | {"mode": "dry-run", "publisher": {"writes": 0}}
