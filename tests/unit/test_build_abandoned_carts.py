from src.transform.build_abandoned_carts import build_abandoned_carts


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
