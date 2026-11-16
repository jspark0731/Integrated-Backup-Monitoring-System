"""Daily NetWorker results from Workflow outcomes; no I/O or Action outcome roll-up."""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
from urllib.parse import quote
from typing import Any

from apps.core.config import NetworkerCurrentConfig
from apps.parsers.networker_rest_parser import _action_evidence

# A fixed UTC+09 offset avoids requiring an OS tzdata package in minimal images.
KST = timezone(timedelta(hours=9), "Asia/Seoul")
ACTIVE = {"active", "started", "sessionactive", "queued", "running", "pending"}
RESULTS = {
    "succeeded": ("success", "success"), "success": ("success", "success"),
    "failed": ("failed", "failed"), "failure": ("failed", "failed"),
    "canceled": ("failed", "canceled"), "cancelled": ("failed", "canceled"),
    "aborted": ("failed", "aborted"), "abandoned": ("failed", "aborted"),
    "communicationlost": ("failed", "needs_attention"),
    "unknown": ("failed", "needs_attention"),
    "neverstarted": ("failed", "not_run"), "didnotrun": ("failed", "not_run"),
    "missedtheschedule": ("failed", "not_run"),
}


def timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else None
    except ValueError:
        return None


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def window(day: str) -> tuple[datetime, datetime]:
    start = datetime.combine(date.fromisoformat(day), time(8), KST)
    return start.astimezone(timezone.utc), (start + timedelta(days=1)).astimezone(timezone.utc)


def bucket(value: datetime) -> str:
    return (value.astimezone(KST) - timedelta(hours=8)).date().isoformat()


def closed_days(now: datetime, source: str, config: NetworkerCurrentConfig) -> list[str]:
    latest = date.fromisoformat(bucket(now)) - timedelta(days=1)
    days = {(latest - timedelta(days=i)).isoformat() for i in range(config.rebuild_days)}
    for key, proof in config.run_evidence.items():
        if (key.startswith(source + ":") and isinstance(proof, dict) and proof.get("backup_day")
                and isinstance(proof.get("reference"), str) and proof["reference"].strip()
                and timestamp(proof.get("start_time"))):
            try:
                day = date.fromisoformat(proof["backup_day"])
                if day <= latest:
                    days.add(day.isoformat())
            except (ValueError, TypeError):
                raise ValueError("invalid evidence backup_day")
    return sorted(days)


def valid_proof(job: dict, source: str, evidence: dict) -> dict:
    proof = evidence.get(f"{source}:{job.get('job_id')}", {})
    if not isinstance(proof, dict) or not isinstance(proof.get("reference"), str) or not proof["reference"].strip():
        return {}
    # Source+ID alone cannot attest an execution if the server reuses an ID.
    start = timestamp(job.get("start_time"))
    return proof if start and timestamp(proof.get("start_time")) == start else {}


def operating_evidence(job: dict, source: str, config: NetworkerCurrentConfig) -> tuple[dict, list[str]]:
    """Exact, effective-dated operating rules; per-run evidence remains optional."""
    policy = str(job.get("policy_name") or job.get("name") or "")
    workflow = str(job.get("workflow_name") or "")
    event = timestamp(job.get("start_time")) or timestamp(job.get("end_time"))
    matches = []
    for rule in config.workflow_rules:
        begin, end = timestamp(rule.get("effective_from")), timestamp(rule.get("effective_until"))
        if (event and begin and event >= begin and (not rule.get("effective_until") or end and event < end)
                and (rule["source_networker"], rule["policy_name"], rule["workflow_name"]) == (source, policy, workflow)):
            matches.append(rule)
    if len(matches) > 1:
        return {}, ["overlapping_workflow_operating_rules"]
    proof = dict(matches[0]) if matches else {}
    proof.update(valid_proof(job, source, config.run_evidence))
    return proof, []


def assigned_day(job: dict, proof: dict) -> tuple[str | None, str]:
    start, end = timestamp(job.get("start_time")), timestamp(job.get("end_time"))
    if proof.get("backup_day"):
        return date.fromisoformat(proof["backup_day"]).isoformat(), "execution_evidence"
    basis = proof.get("backup_day_basis")
    if basis == "start_window" and start:
        return bucket(start), "workflow_start_rule"
    if basis == "completion_window" and end:
        return bucket(end), "workflow_completion_rule"
    if start and end and bucket(start) == bucket(end):
        return bucket(end), "completion_window"
    return None, "unresolved"


def discovered_days(hits: list[dict], source: str, config: NetworkerCurrentConfig, latest: str) -> set[str]:
    days = set()
    for job, _ in select_runs(hits, source):
        evidence, _ = operating_evidence(job, source, config)
        day, _ = assigned_day(job, evidence)
        if day and day <= latest:
            days.add(day)
    return days


def select_runs(hits: list[dict], source: str) -> list[tuple[dict, list[str]]]:
    copies = defaultdict(list)
    for hit in hits:
        doc = hit.get("_source", {})
        p = doc.get("payload", {})
        if doc.get("source_networker") == source and p.get("type", "").lower() == "workflow job":
            if p.get("job_id") is not None:
                copies[str(p["job_id"])].append((p, doc))
    out = []
    for items in copies.values():
        starts = {timestamp(p.get("start_time")) for p, _ in items if timestamp(p.get("start_time"))}
        terminal = [(p, d) for p, d in items if timestamp(p.get("end_time"))
                    or str(p.get("completion_status") or "").lower() in RESULTS
                    or str(p.get("state_raw") or p.get("state") or "").lower() in RESULTS]
        pool = terminal or items
        pool.sort(key=lambda item: (timestamp(item[1].get("observed_at")) or datetime.min.replace(tzinfo=timezone.utc), digest(item[0])))
        job, doc = pool[-1]
        reasons = []
        if len(starts) > 1:
            reasons.append("job_id_reuse_or_start_conflict")
        outcomes = {(str(p.get("completion_status") or "").lower(), timestamp(p.get("end_time"))) for p, _ in terminal}
        if len(outcomes) > 1:
            reasons.append("conflicting_terminal_observations")
        # An omitted message must not erase retained explicit evidence on another copy.
        evidence = {_action_evidence(p.get("message"))["skipped_actions"][0]
                    for p, _ in items if len(_action_evidence(p.get("message"))["skipped_actions"]) == 1}
        if not job.get("message") and evidence:
            job = dict(job, message=next(p["message"] for p, _ in items if _action_evidence(p.get("message"))["skipped_actions"]))
        out.append((dict(job, job_id=str(job["job_id"]), source_observed_at=doc.get("observed_at")), reasons))
    return out


def classify(job: dict, proof: dict, children: list[dict], source: str) -> tuple[str, str, str, list[str]]:
    completion = str(job.get("completion_status") or "").strip().lower()
    state = str(job.get("state_raw") or job.get("state") or "").strip().lower()
    end = timestamp(job.get("end_time"))
    if completion not in RESULTS and (completion in ACTIVE or state in ACTIVE) and not end:
        return "running", "running", "unknown", []
    reasons = []
    evidence = _action_evidence(job.get("message"))
    skipped = set(evidence["skipped_actions"])
    start = timestamp(job.get("start_time"))
    actions = {}
    for c in children:
        if c.get("source_networker") != source or str(c.get("root_parent_job_id")) != str(job["job_id"]):
            continue
        if c.get("type", "").lower() == "backup action job" and str(c.get("parent_job_id")) == str(job["job_id"]):
            cs, ce = timestamp(c.get("start_time")), timestamp(c.get("end_time"))
            if start and cs and cs >= start and (not end or ce and cs <= ce <= end):
                actions[str(c.get("job_id"))] = c
    performed = False
    for c in children:
        parent = actions.get(str(c.get("parent_job_id")))
        if (parent and c.get("source_networker") == source and c.get("type", "").lower() == "save job"
                and str(c.get("root_parent_job_id")) == str(job["job_id"])):
            cs, ce = timestamp(c.get("start_time")), timestamp(c.get("end_time"))
            ps, pe = timestamp(parent.get("start_time")), timestamp(parent.get("end_time"))
            if cs and ps and cs >= ps and (not end or ce and cs <= ce <= end) and (not pe or ce and ce <= pe):
                performed = True
    descendants = [c for c in children if c.get("source_networker") == source
                   and str(c.get("root_parent_job_id")) == str(job["job_id"])
                   and str(c.get("job_id")) != str(job["job_id"])]
    # The dashboard excludes explicitly unperformed backup Actions, not arbitrary
    # skipped Actions or Save targets. The parser requires both matching clauses.
    backup_skip = any(name.casefold() == "backup" for name in skipped)
    outcome = completion if completion in RESULTS else state
    if backup_skip:
        non_skipped_descendants = [c for c in descendants
            if str(c.get("job_id")) not in actions
            or not _action_evidence(c.get("message"))["skipped_actions"]
            or any(name.casefold() != "backup" for name in _action_evidence(c.get("message"))["skipped_actions"])]
        if performed or non_skipped_descendants:
            reasons.append("skip_and_execution_evidence_conflict")
        elif outcome in {"succeeded", "success", "neverstarted", "didnotrun", "missedtheschedule"}:
            return "excluded", "skipped", "unknown", []
    kind = proof.get("workflow_kind")
    if kind == "mixed":
        return "unconfirmed", "mixed_backup_clone", "mixed", ["mixed_workflow_daily_scope_unresolved"]
    if kind == "clone":
        if performed:
            return "unconfirmed", "mixed_backup_clone", "unknown", ["clone_evidence_conflicts_with_backup_activity"]
        return "excluded", "clone", "clone", reasons
    if any(c.get("type", "").lower() not in {"backup action job", "save job", "savefs job"} for c in descendants):
        reasons.append("unsupported_auxiliary_job_not_used_for_outcome")
    if kind not in {"backup", "clone"}:
        # A validated Save proves backup activity, not a Clone type or its outcome.
        kind = "backup" if performed else "unknown"
    result, detail = RESULTS.get(completion, ("unconfirmed", "unrecognized_source_completion"))
    if result == "unconfirmed" and state in RESULTS:
        result, detail = RESULTS[state]
    if not end:
        reasons.append("completion_time_missing")
    if start and end and end < start:
        reasons.append("invalid_execution_times")
    # Missing child evidence, auxiliary types and partial Action Skip do not change
    # the Workflow outcome. Only explicit backup Skip exclusions are applied above.
    qualifications = {"skip_and_execution_evidence_conflict",
                      "unsupported_auxiliary_job_not_used_for_outcome", "completion_time_missing"}
    if reasons and not (result in {"success", "failed"} and set(reasons).issubset(qualifications)):
        return "unconfirmed", detail, kind, reasons
    return result, detail, kind, reasons


def hostname_evidence(job: dict, candidates: list[dict], source: str) -> dict:
    """Explicit Workflow client fields or client names on validated Save descendants.

    Legacy Saves have no provenance marker: use their stored normalized client
    fields, never a bare runOnHost value or a Workflow/Action execution host.
    """
    names, origins = set(), set()

    def target(p, *, saved=False):
        origin = p.get("hostname_source")
        explicit = origin in ("clientName", "clientHostname")
        if not explicit and not (saved and origin in (None, "", "save_job.runOnHost")):
            return None, None, set(), False
        values = [p.get(k) for k in ("client_name", "client_hostname", "_document_client_hostname")]
        claims = {v.strip().lower().rstrip(".") for v in values if isinstance(v, str) and v.strip().rstrip(".")}
        invalid = any(v is not None and v != "" and not isinstance(v, str) for v in values)
        # For the confirmed Save fallback path, a contradictory execution host
        # must not silently win over (or be ignored beside) normalized client data.
        if saved and not explicit:
            host = p.get("run_on_host")
            if isinstance(host, str) and host.strip():
                normalized = host.strip().lower().rstrip(".")
                if claims and claims != {normalized}:
                    claims.add(normalized)
                    invalid = True
            elif host is not None and host != "" and not isinstance(host, str):
                invalid = True
        if len(claims) != 1 or invalid:
            return None, None, claims, invalid or len(claims) > 1
        return next(iter(claims)), origin or "save_job.stored_client", claims, False

    def include(p, *, saved=False):
        value, origin, _, _ = target(p, saved=saved)
        if value:
            names.add(value)
            origins.add(origin)

    include(job)
    start, end = timestamp(job.get("start_time")), timestamp(job.get("end_time"))
    observations = defaultdict(set)
    conflicting = set()
    for c in candidates:
        if c.get("source_networker") == source and c.get("type", "").lower() == "save job":
            _, _, claims, invalid = target(c, saved=True)
            identity = str(c.get("job_id"))
            observations[identity].update(claims)
            if invalid:
                conflicting.add(identity)
    actions = {}
    for c in candidates:
        if (c.get("source_networker") == source and str(c.get("root_parent_job_id")) == str(job["job_id"])
                and str(c.get("parent_job_id")) == str(job["job_id"])
                and c.get("type", "").lower() == "backup action job"):
            cs, ce = timestamp(c.get("start_time")), timestamp(c.get("end_time"))
            if start and cs and cs >= start and (not end or ce and cs <= ce <= end):
                actions.setdefault(str(c.get("job_id")), []).append(c)
    for c in candidates:
        if (c.get("source_networker") != source or c.get("type", "").lower() != "save job"
                or str(c.get("root_parent_job_id")) != str(job["job_id"])):
            continue
        parents = actions.get(str(c.get("parent_job_id")), [])
        # Conflicting parent observations are not a hostname attribution proof.
        intervals = {(p.get("start_time"), p.get("end_time")) for p in parents}
        if len(intervals) != 1:
            continue
        parent = parents[0]
        cs, ce = timestamp(c.get("start_time")), timestamp(c.get("end_time"))
        ps, pe = timestamp(parent.get("start_time")), timestamp(parent.get("end_time"))
        if (str(c.get("job_id")) not in conflicting and len(observations[str(c.get("job_id"))]) <= 1
                and cs and ps and cs >= ps
                and (not end or ce and cs <= ce <= end) and (not pe or ce and ce <= pe)):
            include(c, saved=True)
    ordered = sorted(n for n in names if n)
    return {"hostname": ordered[0] if len(ordered) == 1 else None, "hostnames": ordered,
            "hostname_status": "identified" if len(ordered) == 1 else "multiple" if ordered else "unidentified",
            "hostname_sources": sorted(origins)}


def policy_host_key(source: str, policy: str, hostname: str) -> str:
    # Escaping avoids collisions such as Policy A-B / host C vs A / host B-C.
    return ":".join(quote(v, safe="") for v in (source, policy, hostname))


def daily_failure_view(summary: dict, rows: list[dict]) -> dict:
    """ONE document per day. Terms doc_count is an exact distinct failure-day count."""
    keys = {policy_host_key(r["source_networker"], r["policy_name"], host)
            for r in rows if r["dashboard_result"] == "failed" and r.get("policy_name")
            for host in r.get("hostnames", [])}
    return {k: summary.get(k) for k in (
        "source_networker", "backup_day", "window_start", "window_end", "updated_at", "generation",
        "data_status", "data_reasons", "coverage_status", "aggregation_status")} | {
        "document_kind": "daily_policy_host",
        "projection_status": "ready" if summary.get("failure_count") is not None else "unavailable",
        "failed_policy_host_keys": sorted(keys), "failed_policy_host_count": len(keys),
        "unidentified_failure_count": sum(r["dashboard_result"] == "failed" and not r.get("hostnames") for r in rows),
        "failure_count": summary.get("failure_count"), "workflow_count": summary.get("workflow_count")}


def running_candidates(hits: list[dict], source: str) -> dict[str, dict]:
    """Reuse the same O(N + connected candidates) relationship index as daily work."""
    children, roots, parents = [], defaultdict(list), defaultdict(list)
    for hit in hits:
        d, p = hit.get("_source", {}), hit.get("_source", {}).get("payload", {})
        if d.get("source_networker") != source:
            continue
        i = len(children)
        children.append(p | {"source_networker": source,
                             "_document_client_hostname": d["client"].get("hostname")
                             if isinstance(d.get("client"), dict) else None})
        roots[str(p.get("root_parent_job_id"))].append(i)
        parents[str(p.get("parent_job_id"))].append(i)
    out = {}
    for job, issues in select_runs(hits, source):
        identity = str(job["job_id"])
        positions = set(roots.get(identity, ())) | set(parents.get(identity, ()))
        for i in tuple(positions):
            if children[i].get("type", "").lower() == "backup action job":
                positions.update(parents.get(str(children[i].get("job_id")), ()))
        out[identity] = dict(job, **hostname_evidence(job, [children[i] for i in sorted(positions)], source),
                             relationship_issues=issues)
    return out


def build_daily(hits: list[dict], source: str, days: list[str], now: datetime,
                config: NetworkerCurrentConfig, *, input_reasons: list[str] | None = None) -> dict[str, tuple[dict, list[dict]]]:
    groups = defaultdict(list)
    children = []
    by_root, by_parent = defaultdict(list), defaultdict(list)
    for hit in hits:
        d = hit.get("_source", {})
        p = d.get("payload", {})
        if d.get("source_networker") == source and isinstance(p, dict):
            position = len(children)
            children.append(p | {"source_networker": source,
                                 "_document_client_hostname": d["client"].get("hostname")
                                 if isinstance(d.get("client"), dict) else None})
            by_root[(source, str(p.get("root_parent_job_id")))].append(position)
            by_parent[(source, str(p.get("parent_job_id")))].append(position)
    latest = max(days)
    for job, conflicts in select_runs(hits, source):
        proof, rule_issues = operating_evidence(job, source, config)
        start, end = timestamp(job.get("start_time")), timestamp(job.get("end_time"))
        key = (source, str(job["job_id"]))
        positions = set(by_root.get(key, ())) | set(by_parent.get(key, ()))
        # Retain parent-only children as candidates without inventing a valid root.
        # Include direct Action children too; classify keeps all original checks.
        for position in tuple(positions):
            child = children[position]
            kind = child.get("type")
            if isinstance(kind, str) and kind.lower() == "backup action job":
                positions.update(by_parent.get((source, str(child.get("job_id"))), ()))
        # Positions preserve input order and distinct copies. Only duplicate index
        # references to the SAME input item are removed, never Job observations.
        candidates = [children[position] for position in sorted(positions)]
        result, detail, kind, reasons = classify(job, proof, candidates, source)
        reasons = conflicts + rule_issues + reasons
        if conflicts or rule_issues:
            result = "unconfirmed"
        day, attribution = assigned_day(job, proof)
        if not day and result == "running" and start:
            day, attribution = bucket(start), "running_start_window_only"
        elif not day:
            result, attribution = "unconfirmed", "unresolved"
            reasons.append("original_backup_day_unresolved")
            day = latest  # Diagnostic row only: never a performance attribution.
        if day not in days:
            continue
        policy = str(job.get("policy_name") or job.get("name") or "")
        workflow = str(job.get("workflow_name") or "")
        if not policy or not workflow:
            result = "unconfirmed"
            reasons.append("workflow_identity_missing")
        if start and start > now or end and end > now:
            result = "unconfirmed"
            reasons.append("future_execution_time")
        duration = (end - start).total_seconds() if start and end and end >= start else None
        record = {
            "source_networker": source, "policy_name": policy or None, "workflow_name": workflow or None,
            "backup_day": day if attribution != "unresolved" else None,
            "report_day": day, "attribution": attribution, "workflow_job_id": job["job_id"],
            "source_completion_status": job.get("completion_status") or None,
            "source_state": job.get("state_raw") or job.get("state") or None,
            "source_status_normalized": job.get("status_normalized") or None,
            "source_observed_at": job.get("source_observed_at"),
            "dashboard_result": result, "result_detail": detail, "workflow_kind": kind,
            "start_time": job.get("start_time"), "end_time": job.get("end_time"), "duration_seconds": duration,
            "warning": proof.get("warning") if type(proof.get("warning")) is bool else None,
            "analysis_reasons": sorted(set(reasons)), "evidence_reference": proof.get("reference"),
            "workflow_kind_confirmed": kind != "unknown",
            "action_evidence": _action_evidence(job.get("message")),
            **hostname_evidence(job, candidates, source),
        }
        key = (day, policy, workflow) if policy and workflow and attribution != "unresolved" else (day, "unresolved", job["job_id"])
        groups[key].append(record)
    details_by_day = defaultdict(list)
    for (day, _, _), runs in groups.items():
        # A later skipped/running execution cannot erase the last completed result.
        eligible = [r for r in runs if r["dashboard_result"] not in {"running", "excluded"}]
        pool = eligible or runs
        pool.sort(key=lambda r: (timestamp(r["end_time"]) or timestamp(r["start_time"]) or datetime.min.replace(tzinfo=timezone.utc), r["workflow_job_id"]))
        selected = dict(pool[-1])
        rank = timestamp(selected["end_time"]) or timestamp(selected["start_time"])
        ties = [r for r in pool if (timestamp(r["end_time"]) or timestamp(r["start_time"])) == rank]
        if len(ties) > 1:
            selected["dashboard_result"] = "unconfirmed"
            selected["analysis_reasons"] = sorted(set(selected["analysis_reasons"] + ["final_execution_order_ambiguous"]))
        selected["observed_execution_count"] = len(runs)
        selected["excluded_execution_count"] = sum(r["dashboard_result"] in {"excluded", "running"} for r in runs)
        selected["selection_reason"] = "latest_nonexcluded_completion" if eligible else "excluded_or_incomplete_only"
        selected["execution_ids"] = sorted(r["workflow_job_id"] for r in runs)
        selected["execution_results"] = [{k: r[k] for k in (
            "workflow_job_id", "dashboard_result", "result_detail", "start_time", "end_time",
            "attribution", "analysis_reasons", "evidence_reference", "action_evidence")}
            for r in sorted(runs, key=lambda r: r["workflow_job_id"])]
        details_by_day[day].append(selected)
    output = {}
    for day in days:
        rows = sorted(details_by_day[day], key=lambda r: (str(r["policy_name"]), str(r["workflow_name"]), r["workflow_job_id"]))
        success = [r for r in rows if r["dashboard_result"] == "success"]
        failed = [r for r in rows if r["dashboard_result"] == "failed"]
        completed = success + failed
        durations = [r["duration_seconds"] for r in completed if r["duration_seconds"] is not None]
        reasons = list(input_reasons or [])
        if not config.coverage.get(f"{source}:{day}"):
            reasons.append("api_population_coverage_not_attested")
        if any(r["dashboard_result"] == "unconfirmed" for r in rows):
            reasons.append("analysis_unconfirmed_workflows")
        if any(r["analysis_reasons"] for r in rows if r["dashboard_result"] in {"success", "failed"}):
            reasons.append("classified_results_have_evidence_qualifications")
        if any(not r["workflow_kind_confirmed"] for r in rows if r["dashboard_result"] in {"success", "failed"}):
            reasons.append("workflow_kind_evidence_incomplete")
        warning_known = all(r["warning"] is not None for r in success)
        if not warning_known:
            reasons.append("success_warning_evidence_incomplete")
        if len(durations) != len(completed):
            reasons.append("duration_samples_incomplete")
        starts, ends = window(day)
        summary = {
            "document_kind": "daily_summary", "source_networker": source, "backup_day": day,
            "window_start": starts.isoformat(), "window_end": ends.isoformat(), "updated_at": now.isoformat(),
            "success_count": len(success), "failure_count": len(failed),
            "warning_success_count": sum(r["warning"] is True for r in success) if warning_known else None,
            "success_rate": len(success) / len(completed) if completed else None,
            "duration_seconds_sum": sum(durations) if durations else None,
            "duration_sample_count": len(durations),
            "duration_seconds_average": sum(durations) / len(durations) if durations else None,
            "last_actual_success_at": max((r["end_time"] for r in success if timestamp(r["end_time"])),
                                          key=lambda v: timestamp(v), default=None),
            "excluded_workflow_count": sum(r["dashboard_result"] in {"excluded", "running"} for r in rows),
            "excluded_execution_count": sum(r["excluded_execution_count"] for r in rows),
            "unconfirmed_workflow_count": sum(r["dashboard_result"] == "unconfirmed" for r in rows),
            "workflow_count": len(rows), "data_status": "partial" if reasons else "complete",
            "aggregation_status": "ok",
            "coverage_status": "attested" if config.coverage.get(f"{source}:{day}") else "unverified",
            "data_reasons": sorted(set(reasons)), "coverage_reference": config.coverage.get(f"{source}:{day}"),
            "success_rate_scope": "classified_observed_workflows", "duration_definition": "selected_execution",
        }
        semantic_rows = [{k: v for k, v in r.items() if k != "source_observed_at"} for r in rows]
        generation = digest([semantic_rows, {k: v for k, v in summary.items() if k != "updated_at"}])[:24]
        if not rows and not config.coverage.get(f"{source}:{day}"):
            # No stored observation is not proof that the real population is empty.
            summary.update(success_count=None, failure_count=None, warning_success_count=None)
            generation = digest([semantic_rows, {k: v for k, v in summary.items() if k != "updated_at"}])[:24]
        summary["generation"] = generation
        for row in rows:
            row.update(document_kind="workflow_result", generation=generation, updated_at=now.isoformat(),
                       window_start=starts.isoformat(), window_end=ends.isoformat())
        output[day] = (summary, rows)
    return output
