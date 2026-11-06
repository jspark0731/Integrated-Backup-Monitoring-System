"""Non-destructive naming rules; no reuse of the lossy Parser helpers."""

import re

from .models import Name

NORMALIZATION_VERSION = "trailing-ascii-numeric-v1"
SUFFIX = re.compile(r"(.+):\s*([0-9]+)\s*", re.ASCII)


def normalize_name(value: str, *, strip_suffix: bool) -> Name:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("empty_or_invalid_name")
    text = value.strip()
    if strip_suffix and re.fullmatch(r":\s*[0-9]+", text, flags=re.ASCII):
        raise ValueError("empty_name_before_suffix")
    match = SUFFIX.fullmatch(text) if strip_suffix else None
    if match:
        name = match[1].strip()
        if not name:
            raise ValueError("empty_name_before_suffix")
        return Name(value, name, match[2])
    return Name(value, text)
