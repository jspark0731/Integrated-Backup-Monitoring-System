"""Job Run execution checks, independent of Backup candidate naming."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .models import positive_id, source_of, timestamp, unwrap

ACTION_TYPE = "backup action job"
TERMINAL = {"success", "failed", "canceled"}


@dataclass(frozen=True)
class Job:
    source: str
    identity: str
    kind: str
    parent: str | None
    root: str | None
    status: str
    start: Any
    end: Any
    observed_at: Any
    reference: dict[str, Any]


def read_job(hit: dict[str, Any]) -> Job:
    document, reference = unwrap(hit)
    source = source_of(document)
    payload = document.get("payload")
    if not isinstance(payload, dict) or document.get("document_type") != "job-run":
        raise ValueError("not_a_job_run")
    identity = positive_id(payload.get("job_id"))
    if not identity:
        raise ValueError("invalid_job_id")
    kind = payload.get("type")
    status = payload.get("status_normalized")
    if not isinstance(kind, str) or not isinstance(status, str):
        raise ValueError("invalid_job_type_or_status")
    return Job(source, identity, kind.strip().lower(), positive_id(payload.get("parent_job_id")),
               positive_id(payload.get("root_parent_job_id")), status.strip().lower(),
               timestamp(payload.get("start_time")), timestamp(payload.get("end_time")),
               timestamp(document.get("observed_at")), reference)


def select_job(jobs: list[Job]) -> tuple[Job | None, set[str]]:
    if not jobs:
        return None, {"missing_action_job"}
    reasons: set[str] = set()
    if len({j.start for j in jobs}) > 1 or len({j.kind for j in jobs}) > 1:
        reasons.add("job_id_reuse_or_execution_conflict")
    if len({(j.parent, j.root) for j in jobs}) > 1:
        reasons.add("job_hierarchy_conflict")
    terminal = [j for j in jobs if j.status in TERMINAL]
    if len({(j.status, j.end) for j in terminal}) > 1:
        reasons.add("terminal_state_conflict")
    # Terminal wins over even a later observed Running snapshot. No Source write.
    pool = terminal or jobs
    job = max(pool, key=lambda j: (j.observed_at.isoformat() if j.observed_at else "", j.status))
    if job.start is None or job.observed_at is None or job.status not in TERMINAL | {"running", "unknown"}:
        reasons.add("invalid_job_time_or_status")
    if job.end is not None and (job.start is None or job.end < job.start):
        reasons.add("invalid_job_time_or_status")
    if job.status in TERMINAL and job.end is None:
        reasons.add("invalid_job_time_or_status")
    if job.observed_at and ((job.end and job.observed_at < job.end) or (job.start and job.observed_at < job.start)):
        reasons.add("invalid_job_time_or_status")
    return job, reasons


def validate_save(save: Job, action: Job) -> set[str]:
    """Diagnostic only: never associate a mismatching Save by client or root."""
    reasons: set[str] = set()
    if save.kind != "save job":
        reasons.add("not_a_save_job")
    if save.source != action.source:
        reasons.add("source_mismatch")
    if save.parent != action.identity:
        reasons.add("save_parent_mismatch")
    if action.kind != ACTION_TYPE:
        reasons.add("target_type_mismatch")
    if save.root is not None and action.root is not None and save.root != action.root:
        reasons.add("save_root_conflict")
    if save.start and action.start and save.start < action.start:
        reasons.add("save_time_conflict")
    if save.end and action.end and save.end > action.end:
        reasons.add("save_time_conflict")
    return reasons


def execution_check(key: tuple[str, str], jobs: dict[tuple[str, str], list[Job]]) -> tuple[Job | None, set[str], set[str]]:
    action, blocked = select_job(jobs.get(key, []))
    pending: set[str] = set()
    if action is None:
        return None, set(), blocked
    if action.kind != ACTION_TYPE:
        blocked.add("target_type_mismatch")
    if action.status not in TERMINAL:
        pending.add("action_not_terminal")
    if not action.root:
        pending.add("workflow_root_unconfirmed")
    else:
        workflows = jobs.get((action.source, action.root), [])
        workflow, conflicts = select_job(workflows)
        if workflow is None:
            pending.add("workflow_job_missing")
        else:
            blocked.update(conflicts)
            if workflow.kind != "workflow job":
                blocked.add("workflow_type_mismatch")
            if workflow.start and action.start and action.start < workflow.start:
                blocked.add("workflow_time_conflict")
            if workflow.end and action.end and action.end > workflow.end:
                blocked.add("workflow_time_conflict")
    for copies in jobs.values():
        # Only explicitly referencing Saves participate. Others are not guessed.
        related = [j for j in copies if j.kind == "save job" and j.source == key[0] and j.parent == key[1]]
        if related:
            save, conflicts = select_job(copies)
            blocked.update(conflicts)
            blocked.update(validate_save(save, action))
    return action, blocked, pending
