from __future__ import annotations


def validate_source_networker(value: str | None) -> str:
    """Return the configured domain; never infer a default from a host or name."""
    source = value.strip().lower() if isinstance(value, str) else ""
    if source not in {"core", "chnl", "info"}:
        raise ValueError(f"unsupported source_networker: {value}")
    return source
