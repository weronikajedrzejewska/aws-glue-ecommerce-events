from src.glue_jobs.curate_events import deduplicate_events, flatten_event, is_valid_event


def test_flatten_event_extracts_payload_and_event_date() -> None:
    raw_event = {
        "event_id": "evt-1",
        "event_type": "add_to_cart",
        "event_timestamp": "2026-04-20T10:15:00+00:00",
        "ingestion_timestamp": "2026-04-20T10:20:00+00:00",
        "event_version": "v2",
        "user_id": "user-1",
        "session_id": "sess-1",
        "country": "PL",
        "event_source": "web",
        "device_type": "mobile",
        "payload": {
            "product_id": "prod-1",
            "category": "books",
            "price": 49.99,
            "currency": "USD",
            "quantity": 1,
            "cart_value": 49.99,
        },
    }

    flattened = flatten_event(raw_event)

    assert flattened["event_date"] == "2026-04-20"
    assert flattened["product_id"] == "prod-1"
    assert flattened["device_type"] == "mobile"


def test_is_valid_event_rejects_invalid_purchase_without_price() -> None:
    event = {
        "event_id": "evt-2",
        "event_type": "purchase",
        "event_timestamp": "2026-04-20T10:15:00+00:00",
        "ingestion_timestamp": "2026-04-20T10:20:00+00:00",
        "event_date": "2026-04-20",
        "price": None,
    }

    assert not is_valid_event(event)


def test_deduplicate_events_keeps_latest_ingestion_then_latest_event_time() -> None:
    older = {
        "event_id": "evt-3",
        "event_timestamp": "2026-04-20T10:15:00+00:00",
        "ingestion_timestamp": "2026-04-20T10:20:00+00:00",
    }
    newer = {
        "event_id": "evt-3",
        "event_timestamp": "2026-04-20T10:16:00+00:00",
        "ingestion_timestamp": "2026-04-20T10:21:00+00:00",
    }

    deduplicated = deduplicate_events([older, newer])

    assert deduplicated == [newer]
