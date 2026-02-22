"""Test next_trade_date: weekday, weekend, and Japanese holiday cases."""
from datetime import date

import pytest

from inga_quant.pipeline.trade_date import is_business_day, next_trade_date


class TestIsBusinessDay:
    def test_regular_weekday(self):
        # 2026-02-09 Monday
        assert is_business_day(date(2026, 2, 9)) is True

    def test_saturday(self):
        assert is_business_day(date(2026, 2, 7)) is False

    def test_sunday(self):
        assert is_business_day(date(2026, 2, 8)) is False

    def test_national_foundation_day(self):
        # 2026-02-11 is 建国記念の日
        assert is_business_day(date(2026, 2, 11)) is False

    def test_new_year(self):
        assert is_business_day(date(2026, 1, 1)) is False


class TestNextTradeDate:
    def test_monday_to_tuesday(self):
        # 2026-02-09 Mon → 2026-02-10 Tue
        assert next_trade_date(date(2026, 2, 9)) == date(2026, 2, 10)

    def test_friday_to_monday(self):
        # 2026-02-13 Fri → 2026-02-16 Mon (no holiday on Mon)
        result = next_trade_date(date(2026, 2, 13))
        assert result == date(2026, 2, 16)

    def test_saturday_to_monday(self):
        # 2026-02-14 Sat → 2026-02-16 Mon
        assert next_trade_date(date(2026, 2, 14)) == date(2026, 2, 16)

    def test_sunday_to_monday(self):
        # 2026-02-15 Sun → 2026-02-16 Mon
        assert next_trade_date(date(2026, 2, 15)) == date(2026, 2, 16)

    def test_day_before_holiday(self):
        # 2026-02-10 Tue, next is Wed 2026-02-11 (holiday) → skip to Thu 2026-02-12
        assert next_trade_date(date(2026, 2, 10)) == date(2026, 2, 12)

    def test_result_is_always_business_day(self):
        # Verify for 30 consecutive days that result is always a business day
        start = date(2026, 1, 1)
        for i in range(30):
            d = date.fromordinal(start.toordinal() + i)
            result = next_trade_date(d)
            assert is_business_day(result), f"next_trade_date({d}) = {result} is not a business day"

    def test_result_is_strictly_after_input(self):
        for day_offset in range(14):
            d = date.fromordinal(date(2026, 2, 1).toordinal() + day_offset)
            result = next_trade_date(d)
            assert result > d
