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

    def aggregate(self, _pipeline):
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
    """compute_referral_daily sources joins from vouchers (claim_events) and
    top_inviters from users.referral_count — not from the referrals collection."""
    mongo = _FakeMongo(
        {
            # vouchers: 2 claimed today
            "claim_events": _FakeCollection(
                rows=[{"total": 2}],
            ),
            # users: find() rows for top_inviters; aggregate_rows for total snapshot
            "users": _FakeCollection(
                rows=[{"user_id": "u1", "username": "Alice", "referral_count": 5}],
                aggregate_rows=[{"_id": None, "total": 5}],
            ),
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
    # top_inviters sourced from users.referral_count
    assert result["top_inviters"][0]["referral_count"] == 5
    assert result["top_inviters"][0]["username"] == "Alice"
