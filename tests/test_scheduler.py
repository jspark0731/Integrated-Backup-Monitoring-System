from datetime import datetime, timezone

import pytest

from app.scheduler import seconds_until_next_run


def test_seconds_until_next_run_same_minute() -> None:
    now = datetime(2026, 6, 26, 12, 0, 10, tzinfo=timezone.utc)

    assert seconds_until_next_run(15, now) == 5


def test_seconds_until_next_run_next_minute() -> None:
    now = datetime(2026, 6, 26, 12, 0, 31, tzinfo=timezone.utc)

    assert seconds_until_next_run(30, now) == 59


def test_seconds_until_next_run_rolls_on_exact_second() -> None:
    now = datetime(2026, 6, 26, 12, 0, 0, tzinfo=timezone.utc)

    assert seconds_until_next_run(0, now) == 60


def test_seconds_until_next_run_rejects_invalid_second() -> None:
    with pytest.raises(ValueError):
        seconds_until_next_run(60)

