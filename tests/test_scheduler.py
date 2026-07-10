from datetime import datetime, timezone

import pytest

from app.scheduler import seconds_until_next_run


def test_seconds_until_next_run_dxi_offset_zero() -> None:
    now = datetime(2026, 6, 26, 12, 0, 10, tzinfo=timezone.utc)

    assert seconds_until_next_run(5, 0, 0, now) == 290


def test_seconds_until_next_run_dd_offset_one_minute() -> None:
    now = datetime(2026, 6, 26, 12, 0, 30, tzinfo=timezone.utc)

    assert seconds_until_next_run(5, 1, 0, now) == 30


def test_seconds_until_next_run_i6000_rolls_to_next_window() -> None:
    now = datetime(2026, 6, 26, 12, 2, 0, tzinfo=timezone.utc)

    assert seconds_until_next_run(5, 2, 0, now) == 300


def test_seconds_until_next_run_rejects_invalid_offset() -> None:
    with pytest.raises(ValueError):
        seconds_until_next_run(5, 5, 0)
