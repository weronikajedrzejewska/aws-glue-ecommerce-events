from datetime import datetime, timezone

from src.generator.generate_events import build_base_event, make_dirty_duplicate


def test_build_base_event_adds_device_type_for_v2() -> None:
    event = build_base_event(
        event_type="page_view",
        session_id="sess-1",
        user_id="user-1",
        product_id="prod-1",
        category="books",
        price=15.0,
        event_time=datetime(2026, 4, 20, 10, 0, tzinfo=timezone.utc),
        late_ratio=0.0,
        v2_ratio=1.0,
    )

    assert event["event_version"] == "v2"
    assert "device_type" in event


def test_make_dirty_duplicate_keeps_event_id_and_copies_payload() -> None:
    event = {
        "event_id": "evt-1",
        "event_type": "add_to_cart",
        "event_timestamp": "2026-04-20T10:00:00+00:00",
        "ingestion_timestamp": "2026-04-20T10:10:00+00:00",
        "payload": {
            "price": 25.0,
            "cart_value": 25.0,
        },
    }

    duplicate = make_dirty_duplicate(event)

    assert duplicate["event_id"] == event["event_id"]
    assert duplicate["payload"]["cart_value"] == duplicate["payload"]["price"]
    assert duplicate["payload"] is not event["payload"]
    assert duplicate["ingestion_timestamp"] != event["ingestion_timestamp"]
