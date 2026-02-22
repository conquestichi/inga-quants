"""Trade date calculation: next Japanese business day after a given date."""
from __future__ import annotations

from datetime import date, timedelta

import jpholiday


def is_business_day(d: date) -> bool:
    """Return True if d is a Japanese business day (weekday and not holiday)."""
    return d.weekday() < 5 and not jpholiday.is_holiday(d)


def next_trade_date(as_of: date) -> date:
    """
    Return the next Japanese business day strictly after `as_of`.

    - as_of=Friday   → next Monday (skip weekends, skip holidays)
    - as_of=Saturday → next Monday
    - as_of=Sunday   → next Monday
    - as_of=day before holiday → skips the holiday
    """
    candidate = as_of + timedelta(days=1)
    while not is_business_day(candidate):
        candidate += timedelta(days=1)
    return candidate
