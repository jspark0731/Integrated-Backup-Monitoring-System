"""Validate non-indexed RAW provenance after the complete PIT scan.

The nested summary is required. Existing Builder source_of still checks any
top-level source for agreement; neither index names nor hostnames are evidence.
"""

from apps.classifiers.source_networker import validate_source_networker
from apps.lookups.networker_action.models import source_of

REJECTION_REASONS = ("missing", "empty", "type_error", "mismatch", "conflict", "invalid")


def select_raw_sources(hits, source):
    expected = validate_source_networker(source)
    accepted = []
    counts = {reason: 0 for reason in REJECTION_REASONS}
    for hit in hits:
        document = hit.get("_source", {})
        reason = _rejection(document, expected)
        if reason:
            counts[reason] += 1
        else:
            accepted.append(hit)
    rejected = sum(counts.values())
    return accepted, {
        "raw_hits_total": len(hits),
        "raw_source_accepted": len(accepted),
        "raw_source_rejected": rejected,
        "raw_source_validation_passed": rejected == 0,
        "raw_source_rejection_reasons": counts,
        **{"raw_source_" + name: count for name, count in counts.items()},
    }


def _rejection(document, expected):
    if not isinstance(document, dict):
        return "type_error"
    payload = document.get("payload")
    if payload is None:
        return "missing"
    if not isinstance(payload, dict):
        return "type_error"
    summary = payload.get("summary")
    if summary is None:
        return "missing"
    if not isinstance(summary, dict):
        return "type_error"
    value = summary.get("source_networker")
    if value is None:
        return "missing"
    if not isinstance(value, str):
        return "type_error"
    if not value.strip():
        return "empty"
    try:
        nested = validate_source_networker(value)
    except ValueError:
        return "invalid"
    # Preserve the Builder's requirement that top-level/nested sources agree.
    top = document.get("source_networker")
    if top is not None:
        if not isinstance(top, str):
            return "type_error"
        if not top.strip():
            return "empty"
        try:
            if validate_source_networker(top) != nested:
                return "conflict"
        except ValueError:
            return "invalid"
    if nested != expected:
        return "mismatch"
    # Same normalization and dual-field contract as the unchanged Builder.
    if source_of(document, raw=True) != expected:
        return "conflict"
    return None
