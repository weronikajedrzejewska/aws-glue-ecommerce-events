import json
from pathlib import Path

from src.transform.build_abandoned_carts import build_abandoned_carts, load_curated_events


def test_build_abandoned_carts_marks_completed_purchase() -> None:
    events = [
        {
            "event_type": "add_to_cart",
            "event_timestamp": "2026-04-20T10:00:00+00:00",
            "user_id": "user-1",
            "session_id": "sess-1",
            "product_id": "prod-1",
            "cart_value": 100.0,
        },
        {
            "event_type": "purchase",
            "event_timestamp": "2026-04-20T10:30:00+00:00",
            "user_id": "user-1",
            "session_id": "sess-1",
            "product_id": "prod-1",
            "cart_value": 100.0,
        },
    ]

    result = build_abandoned_carts(events)

    assert len(result) == 1
    assert result[0]["abandoned_cart_flag"] == 0
    assert result[0]["time_to_purchase_minutes"] == 30.0


def test_build_abandoned_carts_ignores_purchase_before_add_to_cart() -> None:
    events = [
        {
            "event_type": "purchase",
            "event_timestamp": "2026-04-20T09:50:00+00:00",
            "user_id": "user-2",
            "session_id": "sess-2",
            "product_id": "prod-2",
            "cart_value": 40.0,
        },
        {
            "event_type": "add_to_cart",
            "event_timestamp": "2026-04-20T10:00:00+00:00",
            "user_id": "user-2",
            "session_id": "sess-2",
            "product_id": "prod-2",
            "cart_value": 40.0,
        },
    ]

    result = build_abandoned_carts(events)

    assert len(result) == 1
    assert result[0]["abandoned_cart_flag"] == 1
    assert result[0]["purchased_ts"] is None


def test_build_abandoned_carts_uses_max_cart_value_for_multiple_adds() -> None:
    events = [
        {
            "event_type": "add_to_cart",
            "event_timestamp": "2026-04-20T10:00:00+00:00",
            "user_id": "user-3",
            "session_id": "sess-3",
            "product_id": "prod-3",
            "cart_value": 20.0,
        },
        {
            "event_type": "add_to_cart",
            "event_timestamp": "2026-04-20T10:05:00+00:00",
            "user_id": "user-3",
            "session_id": "sess-3",
            "product_id": "prod-3",
            "cart_value": 35.0,
        },
    ]

    result = build_abandoned_carts(events)

    assert result[0]["cart_value"] == 35.0
    assert result[0]["has_multiple_adds"] is True


def test_build_abandoned_carts_no_purchase_sets_abandoned_flag() -> None:
    events = [
        {
            "event_type": "add_to_cart",
            "event_timestamp": "2026-04-20T10:00:00+00:00",
            "user_id": "user-4",
            "session_id": "sess-4",
            "product_id": "prod-4",
            "cart_value": 55.0,
        }
    ]

    result = build_abandoned_carts(events)

    assert len(result) == 1
    assert result[0]["abandoned_cart_flag"] == 1
    assert result[0]["purchased_ts"] is None
    assert result[0]["time_to_purchase_minutes"] is None


def test_build_abandoned_carts_two_sessions_same_user_independent() -> None:
    events = [
        # Session A — abandoned
        {
            "event_type": "add_to_cart",
            "event_timestamp": "2026-04-20T10:00:00+00:00",
            "user_id": "user-5",
            "session_id": "sess-A",
            "product_id": "prod-5",
            "cart_value": 50.0,
        },
        # Session B — purchased
        {
            "event_type": "add_to_cart",
            "event_timestamp": "2026-04-20T11:00:00+00:00",
            "user_id": "user-5",
            "session_id": "sess-B",
            "product_id": "prod-5",
            "cart_value": 50.0,
        },
        {
            "event_type": "purchase",
            "event_timestamp": "2026-04-20T11:20:00+00:00",
            "user_id": "user-5",
            "session_id": "sess-B",
            "product_id": "prod-5",
            "cart_value": 50.0,
        },
    ]

    result = build_abandoned_carts(events)
    by_session = {r["session_id"]: r for r in result}

    assert len(result) == 2
    assert by_session["sess-A"]["abandoned_cart_flag"] == 1
    assert by_session["sess-B"]["abandoned_cart_flag"] == 0
    # purchase in sess-B must not close the cart in sess-A
    assert by_session["sess-A"]["purchased_ts"] is None


def test_build_abandoned_carts_two_products_in_one_session() -> None:
    events = [
        {
            "event_type": "add_to_cart",
            "event_timestamp": "2026-04-20T10:00:00+00:00",
            "user_id": "user-6",
            "session_id": "sess-6",
            "product_id": "prod-A",
            "cart_value": 30.0,
        },
        {
            "event_type": "add_to_cart",
            "event_timestamp": "2026-04-20T10:05:00+00:00",
            "user_id": "user-6",
            "session_id": "sess-6",
            "product_id": "prod-B",
            "cart_value": 80.0,
        },
        {
            "event_type": "purchase",
            "event_timestamp": "2026-04-20T10:30:00+00:00",
            "user_id": "user-6",
            "session_id": "sess-6",
            "product_id": "prod-A",
            "cart_value": 30.0,
        },
    ]

    result = build_abandoned_carts(events)
    by_product = {r["product_id"]: r for r in result}

    assert len(result) == 2
    assert by_product["prod-A"]["abandoned_cart_flag"] == 0
    # prod-B has no matching purchase — must be abandoned
    assert by_product["prod-B"]["abandoned_cart_flag"] == 1
    assert by_product["prod-B"]["purchased_ts"] is None


def test_load_curated_events_skips_malformed_json(tmp_path: Path) -> None:
    partition = tmp_path / "event_date=2026-04-20"
    partition.mkdir()
    jsonl = partition / "events.jsonl"
    good = {"event_id": "evt-1", "event_type": "add_to_cart"}
    jsonl.write_text(json.dumps(good) + "\n" + "NOT_JSON\n", encoding="utf-8")

    events = load_curated_events(tmp_path)

    assert len(events) == 1
    assert events[0]["event_id"] == "evt-1"
