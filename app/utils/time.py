from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def day_bounds_utc(target_date: datetime) -> tuple[datetime, datetime]:
    start = datetime(target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def week_bounds_utc(target_date: datetime) -> tuple[datetime, datetime]:
    start = datetime(target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc)
    start = start - timedelta(days=start.weekday())
    end = start + timedelta(days=7)
    return start, end


def week_bounds_utc_for_tz(target_date: datetime, tz_name: str) -> tuple[datetime, datetime]:
    local_tz = ZoneInfo(tz_name)
    local_dt = target_date.astimezone(local_tz)
    week_start_local = datetime(
        local_dt.year, local_dt.month, local_dt.day, tzinfo=local_tz
    ) - timedelta(days=local_dt.weekday())
    week_end_local = week_start_local + timedelta(days=7)
    return week_start_local.astimezone(timezone.utc), week_end_local.astimezone(timezone.utc)


def format_local(dt: datetime, tz_name: str) -> str:
    return dt.astimezone(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M:%S %Z")
