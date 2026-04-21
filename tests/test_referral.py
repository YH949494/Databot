import os

os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault("MONGODB_URI", "mongodb://localhost:27017")
os.environ.setdefault("MONGODB_DB_NAME", "test_db")
os.environ.setdefault("TG_GROWTH_BOT_TOKEN", "token")
os.environ.setdefault("TG_REPORT_CHAT_ID", "-1001")
os.environ.setdefault("TG_ADMIN_USER_IDS", "1")
os.environ.setdefault("TG_CHANNEL_ID", "-1002")

from app.analytics import referral


class _FakeCollection:
    def __init__(self, rows=None, exists_fields=None, aggregate_rows=None):
        self._rows = rows or []
        self._aggregate_rows = aggregate_rows if aggregate_rows is not None else self._rows
        self._exists_fields = exists_fields or set()
        self.aggregate_calls = []

    def aggregate(self, pipeline):
        self.aggregate_calls.append(pipeline)
        return iter(self._aggregate_rows)

    def find(self, *_args, **_kwargs):
        return _FakeCursor(self._rows)

    def find_one(self, query, _projection=None):
        field = next(iter(query.keys()))
        if field in self._exists_fields:
            return {"_id": 1}
        return None


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)

    def sort(self, *_args, **_kwargs):
        return self

    def limit(self, _n):
        return self


class _FakeDerivedCollection:
    def find(self, *_args, **_kwargs):
        return iter([])

    def find_one(self, *_args, **_kwargs):
        return None


class _FakeMongo:
    def __init__(self, source_collections):
        self._source_collections = source_collections
        self.upserts = []
        self.bulk_operations = []

    def source(self, name):
        return self._source_collections[name]

    def derived(self, _name):
        return _FakeDerivedCollection()

    def upsert_one(self, collection, _query, document):
        self.upserts.append((collection, document))

    def bulk_upsert(self, collection, operations):
        self.bulk_operations.append((collection, operations))


def test_referral_daily_uses_voucher_claims_and_user_referral_count() -> None:
    """compute_referral_daily keeps all-time and weekly top inviter metrics separate."""
    users = _FakeCollection(
        rows=[
            {"user_id": "u1", "username": "Alice", "referral_count": 999},
            {"user_id": "u2", "username": "Bob", "referral_count": 5},
        ],
        aggregate_rows=[{"_id": None, "total": 1004}],
    )
    referrals = _FakeCollection(
        aggregate_rows=[
            {"_id": "u2", "referral_count": 3},
            {"_id": "u1", "referral_count": 1},
        ]
    )
    mongo = _FakeMongo(
        {
            # vouchers: 2 claimed today
            "claim_events": _FakeCollection(
                rows=[{"total": 2}],
            ),
            "users": users,
            "referral_events": referrals,
        }
    )
    result = referral.compute_referral_daily(
        mongo, referral.datetime(2026, 1, 11, tzinfo=referral.timezone.utc)
    )
    # joins = daily voucher claims
    assert result["joins"] == 2
    # breakdown not available from vouchers schema
    assert result["qualified"] is None
    assert result.get("joins") == 2  # main assertion — joins sourced from vouchers
    # backward-compatible alias remains all-time from users.referral_count
    assert result["top_inviters"][0]["inviter_user_id"] == "u1"
    assert result["top_inviters"][0]["referral_count"] == 999
    # explicit all-time metric
    assert result["top_inviters_all_time"][0]["inviter_user_id"] == "u1"
    assert result["top_inviters_all_time"][0]["referral_count"] == 999
    # explicit weekly metric from referral_events
    assert result["top_inviters_this_week"][0]["inviter_user_id"] == "u2"
    assert result["top_inviters_this_week"][0]["referral_count"] == 3
    assert result["top_inviters_this_week"][0]["username"] == "Bob"


def test_referral_daily_weekly_top_inviters_uses_kl_monday_boundaries() -> None:
    referrals = _FakeCollection(aggregate_rows=[])
    mongo = _FakeMongo(
        {
            "claim_events": _FakeCollection(rows=[{"total": 0}]),
            "users": _FakeCollection(rows=[], aggregate_rows=[]),
            "referral_events": referrals,
        }
    )

    referral.compute_referral_daily(
        mongo, referral.datetime(2026, 1, 11, 18, 0, tzinfo=referral.timezone.utc)
    )

    match = referrals.aggregate_calls[0][0]["$match"]
    assert match["event_time"]["$gte"].isoformat() == "2026-01-11T16:00:00+00:00"
    assert match["event_time"]["$lt"].isoformat() == "2026-01-18T16:00:00+00:00"


def test_referral_daily_weekly_top_inviters_monday_boundary_excludes_previous_week() -> None:
    referrals = _FakeCollection(aggregate_rows=[])
    mongo = _FakeMongo(
        {
            "claim_events": _FakeCollection(rows=[{"total": 0}]),
            "users": _FakeCollection(rows=[], aggregate_rows=[]),
            "referral_events": referrals,
        }
    )

    referral.compute_referral_daily(
        mongo, referral.datetime(2026, 1, 11, 15, 59, 59, tzinfo=referral.timezone.utc)
    )

    match = referrals.aggregate_calls[0][0]["$match"]
    assert match["event_time"]["$gte"].isoformat() == "2026-01-04T16:00:00+00:00"
    assert match["event_time"]["$lt"].isoformat() == "2026-01-11T16:00:00+00:00"
