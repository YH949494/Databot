import os

os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault("MONGODB_URI", "mongodb://localhost:27017")
os.environ.setdefault("MONGODB_DB_NAME", "test_db")
os.environ.setdefault("TG_GROWTH_BOT_TOKEN", "token")
os.environ.setdefault("TG_REPORT_CHAT_ID", "-1001")
os.environ.setdefault("TG_ADMIN_USER_IDS", "1")
os.environ.setdefault("TG_CHANNEL_ID", "-1002")

from app.analytics.segments import action_for_segment, classify_segment
from app.analytics import segmentation


def test_classify_segment_unknown_when_last_active_missing() -> None:
    assert classify_segment(total_claims=0, last_active_days=None) == "unknown"


def test_classify_segment_new() -> None:
    assert classify_segment(total_claims=5, last_active_days=2) == "new"


def test_classify_segment_high_value_takes_priority_over_active() -> None:
    assert classify_segment(total_claims=30, last_active_days=3) == "high_value"


def test_high_value_overrides_new() -> None:
    assert classify_segment(total_claims=40, last_active_days=2) == "high_value"


def test_classify_segment_active() -> None:
    assert classify_segment(total_claims=10, last_active_days=3) == "active"


def test_classify_segment_at_risk() -> None:
    assert classify_segment(total_claims=10, last_active_days=6) == "at_risk"


def test_classify_segment_dead() -> None:
    assert classify_segment(total_claims=10, last_active_days=8) == "dead"


def test_action_for_each_segment() -> None:
    assert action_for_segment("new") == "onboarding_push"
    assert action_for_segment("active") == "leaderboard_competition"
    assert action_for_segment("at_risk") == "comeback_voucher"
    assert action_for_segment("dead") == "aggressive_reactivation"
    assert action_for_segment("high_value") == "vip_treatment"
    assert action_for_segment("unknown") == "no_action"


class _FakeCollection:
    def __init__(self, rows=None, exists_fields=None):
        self._rows = rows or []
        self._exists_fields = exists_fields or set()

    def aggregate(self, _pipeline):
        return iter(self._rows)

    def find(self, _query, _projection):
        return iter(self._rows)

    def find_one(self, query, _projection):
        field = next(iter(query.keys()))
        if field in self._exists_fields:
            return {"_id": 1}
        return None

class _FakeMongo:
    def __init__(self, collections):
        self._collections = collections
        self.last_upsert = None

    def source(self, name):
        return self._collections[name]

    def bulk_upsert(self, _collection, operations):
        self.operations = operations

    def upsert_one(self, _collection, _filter, document):
        self.last_upsert = document


def test_win_rate_uses_result_count() -> None:
    mongo = _FakeMongo(
        {
            "claim_events": _FakeCollection(
                rows=[
                    {
                        "_id": "u1",
                        "total_claims": 10,
                        "last_claim_at": segmentation.datetime(2026, 1, 10, tzinfo=segmentation.timezone.utc),
                        "first_claim_at": segmentation.datetime(2026, 1, 1, tzinfo=segmentation.timezone.utc),
                        "win_count": 2,
                        "result_count": 2,
                    }
                ],
                exists_fields={"result"},
            ),
            "referral_events": _FakeCollection(rows=[]),
            "users": _FakeCollection(rows=[{"user_id": "u1"}]),
        }
    )
    segmentation.compute_user_profiles(mongo, segmentation.datetime(2026, 1, 11, tzinfo=segmentation.timezone.utc))
    doc = mongo.operations[0][1]
    assert doc["win_loss_pattern"] == "winning"


def test_profiles_only_for_claim_users_and_counts_no_claim_history() -> None:
    mongo = _FakeMongo(
        {
            "claim_events": _FakeCollection(
                rows=[
                    {
                        "_id": "u1",
                        "total_claims": 2,
                        "last_claim_at": segmentation.datetime(2026, 1, 10, tzinfo=segmentation.timezone.utc),
                        "first_claim_at": segmentation.datetime(2026, 1, 5, tzinfo=segmentation.timezone.utc),
                        "win_count": 1,
                        "result_count": 1,
                    }
                ],
                exists_fields={"result"},
            ),
            "referral_events": _FakeCollection(rows=[{"_id": "u3", "referral_count": 2}]),
            "users": _FakeCollection(rows=[{"user_id": "u1"}, {"user_id": "u2"}]),
        }
    )
    counts = segmentation.compute_user_profiles(
        mongo, segmentation.datetime(2026, 1, 11, tzinfo=segmentation.timezone.utc)
    )
    assert len(mongo.operations) == 1
    assert mongo.operations[0][1]["user_id"] == "u1"
    assert counts["no_claim_history"] == 1


def test_cost_per_active_player_logic() -> None:
    mongo = _FakeMongo(
        {
            "claim_events": _FakeCollection(rows=[{"_id": "u1"}, {"_id": "u2"}], exists_fields={"bet_amount", "voucher_value"}),
            "users": _FakeCollection(rows=[]),
        }
    )

    call_index = {"n": 0}

    def _fake_user_ids_for_day(_mongo, _start, _end):
        call_index["n"] += 1
        if call_index["n"] == 1:
            return {"u1"}
        if call_index["n"] == 2:
            return set()
        if call_index["n"] == 3:
            return set()
        if call_index["n"] == 4:
            return set()
        return set()

    original = segmentation._user_ids_for_day
    segmentation._user_ids_for_day = _fake_user_ids_for_day
    try:
        mongo._collections["claim_events"] = _FakeCollection(
            rows=[{"_id": "u1"}, {"_id": "u2"}],
            exists_fields={"bet_amount", "voucher_value"},
        )

        class _ClaimEventsCollection(_FakeCollection):
            def aggregate(self, pipeline):
                if any("$group" in stage and stage["$group"].get("_id") is None for stage in pipeline):
                    return iter([{"_id": None, "voucher_value_sum": 100.0}])
                return iter([{"_id": "u1"}, {"_id": "u2"}])

        mongo._collections["claim_events"] = _ClaimEventsCollection(exists_fields={"bet_amount", "voucher_value"})
        segmentation.compute_segmentation_kpis(mongo, segmentation.datetime(2026, 1, 11, tzinfo=segmentation.timezone.utc))
        assert mongo.last_upsert["cost_per_active_player"] == 100.0
    finally:
        segmentation._user_ids_for_day = original


def test_referral_counts_use_referrals_referrer_user_id() -> None:
    mongo = _FakeMongo(
        {
            "claim_events": _FakeCollection(
                rows=[
                    {
                        "_id": {"user_id": "u1", "username_lower": None},
                        "total_claims": 2,
                        "last_claim_at": segmentation.datetime(2026, 1, 10, tzinfo=segmentation.timezone.utc),
                        "first_claim_at": segmentation.datetime(2026, 1, 1, tzinfo=segmentation.timezone.utc),
                    }
                ]
            ),
            "referral_events": _FakeCollection(rows=[{"_id": "u1", "referral_count": 3}]),
            "users": _FakeCollection(rows=[{"user_id": "u1", "username": "Alpha"}]),
        }
    )
    segmentation.compute_user_profiles(mongo, segmentation.datetime(2026, 1, 11, tzinfo=segmentation.timezone.utc))
    assert mongo.operations[0][1]["referral_count"] == 3


def test_claim_user_linkage_uses_username_lower_read_side_only() -> None:
    mongo = _FakeMongo(
        {
            "claim_events": _FakeCollection(
                rows=[
                    {
                        "_id": {"user_id": None, "username_lower": "alpha"},
                        "total_claims": 4,
                        "last_claim_at": segmentation.datetime(2026, 1, 10, tzinfo=segmentation.timezone.utc),
                        "first_claim_at": segmentation.datetime(2026, 1, 1, tzinfo=segmentation.timezone.utc),
                    }
                ],
                exists_fields={"result"},
            ),
            "referral_events": _FakeCollection(rows=[]),
            "users": _FakeCollection(rows=[{"user_id": "u1", "username": "Alpha"}]),
        }
    )
    segmentation.compute_user_profiles(mongo, segmentation.datetime(2026, 1, 11, tzinfo=segmentation.timezone.utc))
    assert mongo.operations[0][1]["user_id"] == "u1"


def test_segmentation_kpis_claim_users_from_real_claim_source_shape() -> None:
    mongo = _FakeMongo(
        {
            "claim_events": _FakeCollection(
                rows=[{"_id": {"user_id": None, "username_lower": "alpha"}}],
                exists_fields={"bet_amount"},
            ),
            "users": _FakeCollection(rows=[{"user_id": "u1", "username": "Alpha"}]),
        }
    )

    def _fake_user_ids_for_day(_mongo, _start, _end):
        return {"u1"}

    original = segmentation._user_ids_for_day
    segmentation._user_ids_for_day = _fake_user_ids_for_day
    try:
        segmentation.compute_segmentation_kpis(mongo, segmentation.datetime(2026, 1, 11, tzinfo=segmentation.timezone.utc))
        assert mongo.last_upsert["claim_to_play_conversion"] == 1.0
    finally:
        segmentation._user_ids_for_day = original


def test_profiles_use_claimed_at_camel_case_from_voucher_claims() -> None:
    mongo = _FakeMongo(
        {
            "claim_events": _FakeCollection(
                rows=[
                    {
                        "_id": {"user_id": None, "username_lower": "alpha"},
                        "total_claims": 3,
                        "last_claim_at": segmentation.datetime(2026, 1, 10, tzinfo=segmentation.timezone.utc),
                        "first_claim_at": segmentation.datetime(2026, 1, 2, tzinfo=segmentation.timezone.utc),
                    }
                ],
                exists_fields={"claimedAt"},
            ),
            "referral_events": _FakeCollection(rows=[]),
            "users": _FakeCollection(rows=[{"user_id": "u1", "username": "Alpha"}]),
        }
    )
    counts = segmentation.compute_user_profiles(
        mongo, segmentation.datetime(2026, 1, 11, tzinfo=segmentation.timezone.utc)
    )
    assert counts["unknown"] == 0
    assert mongo.operations[0][1]["last_active_days"] == 1


def test_kpis_use_claimed_at_camel_case_and_claimed_by_linkage() -> None:
    mongo = _FakeMongo(
        {
            "claim_events": _FakeCollection(
                rows=[{"_id": {"user_id": None, "username_lower": "alpha"}}],
                exists_fields={"claimedAt", "bet_amount"},
            ),
            "users": _FakeCollection(rows=[{"user_id": "u1", "username": "Alpha"}]),
        }
    )

    original = segmentation._user_ids_for_day
    segmentation._user_ids_for_day = lambda *_args, **_kwargs: {"u1"}
    try:
        segmentation.compute_segmentation_kpis(mongo, segmentation.datetime(2026, 1, 11, tzinfo=segmentation.timezone.utc))
        assert mongo.last_upsert["claim_to_play_conversion"] == 1.0
    finally:
        segmentation._user_ids_for_day = original
