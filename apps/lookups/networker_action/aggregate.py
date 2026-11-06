"""Incremental in-memory evidence ledger and conservative Lookup document builder.

This state must survive partial re-reads. It is NOT a durable production ledger;
rebuilding it from an incomplete input set must never be treated as a refresh.
"""

from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from typing import Any, Iterable

from .extract import extract_raw
from apps.classifiers.source_networker import validate_source_networker

from .models import Coverage, Evidence, Issue, api_timestamp, digest, iso, positive_id, timestamp
from .normalize import NORMALIZATION_VERSION
from .validate import ACTION_TYPE, execution_check, read_job

LOOKUP_INDEX = "nw-ops-lookup-actionrun-v1"


class ActionLookupBuilder:
    """Offline builder: no network calls, Source mutation, or implicit verification."""

    def __init__(self) -> None:
        self.evidence: dict[tuple[str, str], dict[str, Evidence]] = defaultdict(dict)
        self.jobs: dict[tuple[str, str], list] = defaultdict(list)
        self.issues: list[Issue] = []
        self.incomplete_sources: set[str] = set()
        self.unknown_source_errors = False
        self.blocked: dict[tuple[str, str], set[str]] = defaultdict(set)
        self.identities: dict[tuple[str, str], dict[str, tuple[str, str]]] = defaultdict(dict)

    def add_raw(self, hits: Iterable[dict[str, Any]]) -> None:
        for hit in hits:
            extraction = extract_raw(hit)
            self.issues.extend(extraction.issues)
            self.incomplete_sources.update(extraction.incomplete_sources)
            for issue in extraction.issues:
                if issue.source is None:
                    self.unknown_source_errors = True
                elif issue.action_id:
                    self.blocked[(issue.source, issue.action_id)].add("invalid_backup_evidence")
            for item in extraction.evidence:
                key = (item.source, item.action_id)
                attribute_values: dict[str, set[str]] = defaultdict(set)
                for attribute in item.attributes:
                    attribute_values[attribute["key"]].update(attribute["values"])
                fingerprint = digest({"id": item.backup_id, "action": item.action_id,
                                      "original": item.original_candidate,
                                      "attributes": {name: sorted(values) for name, values in attribute_values.items()},
                                      "save_time": item.save_time, "completion_time": item.completion_time})
                if item.backup_id:
                    identity = (item.source, item.backup_id)
                    for old_fingerprint, old_key in self.identities[identity].items():
                        if old_fingerprint != fingerprint:
                            self.blocked[old_key].add("backup_revision_conflict")
                            self.blocked[key].add("backup_revision_conflict")
                    self.identities[identity][fingerprint] = key
                else:
                    # No inferred client+time identity: evidence may be diagnostic
                    # but cannot certify completeness or a unique Backup count.
                    self.blocked[key].add("backup_identity_missing")
                    fingerprint += digest(item.reference)
                previous = self.evidence[key].get(fingerprint)
                if previous is None or (item.observed_at and (previous.observed_at is None or item.observed_at > previous.observed_at)):
                    self.evidence[key][fingerprint] = item

    def add_jobs(self, hits: Iterable[dict[str, Any]]) -> None:
        for hit in hits:
            try:
                job = read_job(hit)
            except ValueError as exc:
                self.issues.append(Issue(str(exc), details=deepcopy(hit)))
                self.unknown_source_errors = True
                continue
            key = (job.source, job.identity)
            if job not in self.jobs[key]:
                self.jobs[key].append(job)
            if job.kind == "save job" and job.parent is None:
                self.issues.append(Issue("invalid_save_parent", job.source, reference=job.reference))

    def build(self, *, synced_at: datetime, coverage: Iterable[Coverage] = (),
              generation: str = "offline-step4a", reference_limit: int = 20) -> dict[str, dict[str, Any]]:
        """Return one _source per action key; keys are the proposed Lookup _ids."""
        if not isinstance(synced_at, datetime) or timestamp(synced_at) is None or type(reference_limit) is not int or reference_limit < 1:
            raise ValueError("aware_synced_at_and_positive_reference_limit_required")
        attestations: dict[tuple[str, str], Coverage] = {}
        for item in coverage:
            source = validate_source_networker(item.source)
            identity = positive_id(item.action_id)
            if identity is None or (source, identity) in attestations:
                raise ValueError("invalid_or_duplicate_coverage_key")
            attestations[(source, identity)] = item
        keys = set(self.evidence) | set(self.blocked)
        keys.update(key for key, copies in self.jobs.items() if any(j.kind == ACTION_TYPE for j in copies))
        keys.update((j.source, j.parent) for copies in self.jobs.values() for j in copies
                    if j.kind == "save job" and j.parent)
        return {f"{key[0]}:{key[1]}": self._document(key, synced_at, attestations.get(key), generation, reference_limit)
                for key in sorted(keys)}

    def _document(self, key: tuple[str, str], synced_at: datetime, coverage: Coverage | None,
                  generation: str, limit: int) -> dict[str, Any]:
        records = list(self.evidence.get(key, {}).values())
        candidates = {e.candidate for e in records}
        originals = {e.original_candidate for e in records}
        action, blocked, pending = execution_check(key, self.jobs)
        blocked.update(self.blocked.get(key, set()))
        if self.unknown_source_errors:
            pending.add("unattributed_input_errors")
        if key[0] in self.incomplete_sources or (coverage and coverage.partial):
            pending.add("partial_collection")
        if coverage and coverage.query_window_gap:
            pending.add("query_window_gap")
        if any(e.observed_at is None for e in records):
            pending.add("missing_observation_time")
        if any(not (e.reference.get("raw_document_id") or (e.reference.get("index") and e.reference.get("id"))) for e in records):
            pending.add("missing_raw_reference")
        # An unresolved suffix policy must not publish even apparently unique names.
        if coverage is None or not coverage.suffix_reviewed:
            pending.add("suffix_rule_unreviewed")
        start, end = (action.start, action.end) if action else (None, None)
        for evidence in records:
            saved, completed = api_timestamp(evidence.save_time), api_timestamp(evidence.completion_time)
            if saved is None or completed is None:
                pending.add("backup_time_unconfirmed")
            elif completed < saved or (start and saved < start) or (end and completed > end):
                blocked.add("backup_execution_time_conflict")
        if any(observed > synced_at for observed in [e.observed_at for e in records] if observed):
            pending.add("future_observation_time")
        if action and action.observed_at and action.observed_at > synced_at:
            pending.add("future_observation_time")
        if coverage is None or not coverage.complete_for(start, end):
            pending.add("coverage_unverified")
        if not records:
            pending.add("no_backup_in_covered_window" if coverage and coverage.complete_for(start, end) else "awaiting_backups")
        mapping = "ambiguous" if len(candidates) > 1 else "resolved" if candidates else "invalid" if blocked else "unresolved"
        verification = "blocked" if blocked else "provisional" if pending else "verified" if mapping == "resolved" else "unverified"
        public = next(iter(candidates)) if mapping == "resolved" and verification == "verified" else (None, None, None)
        reasons = sorted(blocked | pending)
        priority = ["job_id_reuse_or_execution_conflict", "target_type_mismatch", "backup_revision_conflict",
                    "invalid_backup_evidence", "missing_action_job", "partial_collection", "query_window_gap",
                    "awaiting_backups", "no_backup_in_covered_window"]
        state = next((reason for reason in priority if reason in reasons),
                     "ambiguous" if mapping == "ambiguous" else reasons[0] if reasons else "verified")
        observed = [e.observed_at for e in records if e.observed_at]
        if action and action.observed_at:
            observed.append(action.observed_at)
        references = sorted((e.reference for e in records), key=lambda r: digest(r))
        audit = [{"original": list(e.original_candidate), "normalized": list(e.candidate),
                  "policy_suffix": e.policy.suffix, "workflow_suffix": e.workflow.suffix,
                  "attributes": list(deepcopy(e.attributes)), "reference": deepcopy(e.reference)} for e in records[:limit]]
        return {
            "action_run_key": f"{key[0]}:{key[1]}", "source_networker": key[0], "action_job_id": key[1],
            "action_job_type": action.kind if action else None,
            "action_status": action.status if action else None,
            "action_workflow_run_key": f"{key[0]}:{action.root}" if action and action.root else None,
            "policy_name": public[0], "workflow_name": public[1], "protection_group": public[2],
            "mapping_status": mapping, "verification_status": verification, "evidence_state": state,
            "reason": ";".join(reasons), "candidate_count": len(candidates), "raw_candidate_count": len(originals),
            "normalization_version": NORMALIZATION_VERSION,
            "suffix_validation": "reviewed" if coverage and coverage.suffix_reviewed else "unreviewed",
            "lookup_generation": generation, "candidate_fingerprint": digest(sorted(candidates)),
            "action_start_time": iso(start), "action_end_time": iso(end),
            "lookup_observed_at": iso(max(observed)) if observed else None,
            "lookup_synced_at": iso(synced_at),
            "coverage_from": iso(coverage.start) if coverage else None,
            "coverage_to": iso(coverage.end) if coverage else None,
            "backup_reference_count": len(references), "backup_reference_truncated": len(references) > limit,
            "backup_references": {"items": deepcopy(references[:limit])},
            "candidate_evidence": {"items": audit, "truncated": len(records) > limit,
                                   "coverage_proof": coverage.proof_reference if coverage else ""},
        }
