from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class CollectionResult:
    collector: str
    target_type: str
    protocol: str
    collected_at: datetime
    ok: bool
    payload: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    skipped: bool = False
    skip_reason: str | None = None

    @classmethod
    def skipped_result(cls, collector: str, target_type: str, protocol: str, reason: str) -> "CollectionResult":
        return cls(
            collector=collector,
            target_type=target_type,
            protocol=protocol,
            collected_at=datetime.now(timezone.utc),
            ok=False,
            skipped=True,
            skip_reason=reason,
        )

    def to_document(self) -> dict[str, Any]:
        return {
            "@timestamp": self.collected_at.astimezone(timezone.utc).isoformat(),
            "collector": self.collector,
            "target_type": self.target_type,
            "protocol": self.protocol,
            "ok": self.ok,
            "payload": self.payload,
            "error": self.error,
            "skipped": self.skipped,
            "skip_reason": self.skip_reason,
        }

