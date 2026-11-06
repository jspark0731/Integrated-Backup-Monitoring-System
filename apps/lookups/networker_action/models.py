"""Value objects shared by extraction, validation and offline aggregation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import re
import math
from typing import Any

from apps.classifiers.source_networker import validate_source_networker


def positive_id(value: Any) -> str | None:
    # Preserve canonical API spelling, rather than silently rewriting leading zeroes.
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    text = str(value).strip()
    return text if re.fullmatch(r"[1-9][0-9]*", text, flags=re.ASCII) else None


def timestamp(value: Any) -> datetime | None:
    if not isinstance(value, (str, datetime)):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
        # Ambiguous local dates must not establish execution coverage.
        return parsed.astimezone(timezone.utc) if parsed.tzinfo is not None else None
    except (ValueError, OverflowError):
        return None


def iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def api_timestamp(value: Any) -> datetime | None:
    """Backup API accepts seconds/milliseconds as in the existing Parser contract."""
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
        if not math.isfinite(number):
            return None
        return datetime.fromtimestamp(number / 1000 if abs(number) >= 1e12 else number, timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError):
        return timestamp(value)


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def source_of(document: dict[str, Any], *, raw: bool = False) -> str:
    payload = document.get("payload")
    payload = payload if isinstance(payload, dict) else {}
    nested = payload.get("summary") if raw else payload
    nested = nested if isinstance(nested, dict) else {}
    values = [validate_source_networker(v) for v in
              [document.get("source_networker"), nested.get("source_networker")] if v is not None]
    if not values or len(set(values)) != 1:
        raise ValueError("missing_or_conflicting_source_networker")
    return values[0]


def unwrap(hit: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(hit, dict):
        raise ValueError("invalid_source_document")
    document = hit.get("_source", hit)
    if not isinstance(document, dict):
        raise ValueError("invalid_source_document")
    return document, {"index": hit.get("_index"), "id": hit.get("_id")}


@dataclass(frozen=True)
class Name:
    original: str
    normalized: str
    suffix: str | None = None


@dataclass(frozen=True)
class Issue:
    reason: str
    source: str | None = None
    action_id: str | None = None
    reference: dict[str, Any] = field(default_factory=dict)
    details: Any = None


@dataclass(frozen=True)
class Evidence:
    source: str
    action_id: str
    backup_id: str | None
    policy: Name
    workflow: Name
    group: Name
    attributes: tuple[dict[str, Any], ...]
    reference: dict[str, Any]
    observed_at: datetime | None
    save_time: Any = None
    completion_time: Any = None

    @property
    def candidate(self) -> tuple[str, str, str]:
        return (self.policy.normalized, self.workflow.normalized, self.group.normalized)

    @property
    def original_candidate(self) -> tuple[str, str, str]:
        return (self.policy.original, self.workflow.original, self.group.original)


@dataclass(frozen=True)
class Coverage:
    """Explicit action-scoped attestation, never inferred from a successful poll.

    The caller must establish completeness outside this module, including gaps
    between API windows. The default cannot authorize public relationship names.
    """

    source: str
    action_id: str
    start: datetime | None = None
    end: datetime | None = None
    jobs_complete: bool = False
    backups_complete: bool = False
    partial: bool = False
    query_window_gap: bool = False
    suffix_reviewed: bool = False
    proof_reference: str = ""

    def __post_init__(self) -> None:
        validate_source_networker(self.source)
        if positive_id(self.action_id) is None or not isinstance(self.proof_reference, str):
            raise ValueError("invalid_coverage_identity_or_proof")
        for flag in [self.jobs_complete, self.backups_complete, self.partial, self.query_window_gap, self.suffix_reviewed]:
            if type(flag) is not bool:
                raise ValueError("coverage_flags_must_be_boolean")
        for bound in [self.start, self.end]:
            if bound is not None and (not isinstance(bound, datetime) or bound.tzinfo is None):
                raise ValueError("coverage_bounds_must_be_aware_datetimes")

    def complete_for(self, start: datetime | None, end: datetime | None) -> bool:
        bounds = [self.start, self.end, start, end]
        return bool(
            all(isinstance(t, datetime) and t.tzinfo is not None for t in bounds)
            and self.jobs_complete and self.backups_complete
            and not self.partial and not self.query_window_gap
            and self.proof_reference.strip()
            and self.start <= start <= end <= self.end
        )
