import os
from datetime import datetime, timezone

os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault("MONGODB_URI", "mongodb://localhost:27017")
os.environ.setdefault("MONGODB_DB_NAME", "test_db")
os.environ.setdefault("TG_GROWTH_BOT_TOKEN", "token")
os.environ.setdefault("TG_REPORT_CHAT_ID", "-1001")
os.environ.setdefault("TG_ADMIN_USER_IDS", "1")
os.environ.setdefault("TG_CHANNEL_ID", "-1002")

from app.analytics import referral


class _FakeCollection:
    def __init__(self, rows):
        self._rows = rows

    def find(self, *_args, **_kwargs):
        return iter(self._rows)

    def find_one(self, *_args, **_kwargs):
        return None


class _FakeMongo:
    def __init__(self, daily_rows, inviter_rows):
        self._daily = daily_rows
        self._inviter = inviter_rows
        self.upserts = []

    def derived(self, name):
        if name == "referral_daily":
            return _FakeCollection(self._daily)
        if name == "inviter_daily":
            return _FakeCollection(self._inviter)
        if name == "referral_weekly":
            return _FakeCollection([])
        raise KeyError(name)

    def upsert_one(self, collection, _query, document):
        self.upserts.append((collection, document))


def test_compute_referral_weekly_uses_delta_when_inviter_daily_joins_is_cumulative_snapshot() -> None:
    # inviter_daily.joins is written from users.referral_count (cumulative snapshot),
    # so weekly joins must use delta, not sum.
    week_start = datetime(2026, 1, 5, tzinfo=timezone.utc)
    mongo = _FakeMongo(
        daily_rows=[{"date": week_start, "joins": 12, "qualified": 0}],
        inviter_rows=[
            {"inviter_user_id": "u1", "username": "Alice", "date": datetime(2026, 1, 4, tzinfo=timezone.utc), "joins": 10},
            {"inviter_user_id": "u1", "username": "Alice", "date": datetime(2026, 1, 6, tzinfo=timezone.utc), "joins": 15},
            {"inviter_user_id": "u2", "username": "Bob", "date": datetime(2026, 1, 6, tzinfo=timezone.utc), "joins": 8},
            {"inviter_user_id": "u2", "username": "Bob", "date": datetime(2026, 1, 10, tzinfo=timezone.utc), "joins": 11},
        ],
    )

    summary = referral.compute_referral_weekly(
        mongo, datetime(2026, 1, 11, tzinfo=timezone.utc)
    )

    assert summary["top_inviters"][0]["inviter_user_id"] == "u1"
    assert summary["top_inviters"][0]["joins"] == 5
    assert summary["top_inviters"][1]["inviter_user_id"] == "u2"
    assert summary["top_inviters"][1]["joins"] == 3


def test_compute_referral_weekly_delta_fallback_without_preweek_baseline() -> None:
    week_start = datetime(2026, 1, 5, tzinfo=timezone.utc)
    mongo = _FakeMongo(
        daily_rows=[{"date": week_start, "joins": 6, "qualified": 0}],
        inviter_rows=[
            {"inviter_user_id": "u1", "username": "Alice", "date": datetime(2026, 1, 6, tzinfo=timezone.utc), "joins": 10},
        ],
    )

    summary = referral.compute_referral_weekly(
        mongo, datetime(2026, 1, 11, tzinfo=timezone.utc)
    )

    assert summary["top_inviters"][0]["inviter_user_id"] == "u1"
    assert summary["top_inviters"][0]["joins"] == 0
