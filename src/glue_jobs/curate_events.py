import json
from datetime import datetime
from pathlib import Path


RAW_DIR = Path("data/raw/events")
CURATED_DIR = Path("data/curated/events")


def load_raw_events() -> list[dict]:
    events = []
    for file_path in RAW_DIR.rglob("*.jsonl"):
        with file_path.open("r", encoding="utf-8") as f:
            for line in f:
                events.append(json.loads(line))
    return events


def flatten_event(event: dict) -> dict:
    payload = event.get("payload", {})
    event_ts = datetime.fromisoformat(event["event_timestamp"])

    return {
        "event_id": event.get("event_id"),
        "event_type": event.get("event_type"),
        "event_timestamp": event.get("event_timestamp"),
        "ingestion_timestamp": event.get("ingestion_timestamp"),
        "event_date": event_ts.date().isoformat(),
        "event_version": event.get("event_version"),
        "user_id": event.get("user_id"),
        "session_id": event.get("session_id"),
        "country": event.get("country"),
        "event_source": event.get("event_source"),
        "device_type": event.get("device_type"),
        "product_id": payload.get("product_id"),
        "category": payload.get("category"),
        "price": payload.get("price"),
        "currency": payload.get("currency"),
        "quantity": payload.get("quantity"),
        "cart_value": payload.get("cart_value"),
    }


def is_valid_event(event: dict) -> bool:
    if event["event_id"] is None:
        return False
    if event["event_timestamp"] is None:
        return False
    if event["event_type"] in {"add_to_cart", "purchase"} and event["price"] is None:
        return False
    return True


def deduplicate_events(events: list[dict]) -> list[dict]:
    latest_by_event_id = {}

    for event in events:
        event_id = event["event_id"]
        current = latest_by_event_id.get(event_id)

        # Last-write-wins by ingestion time; event time is used as a tiebreaker.
        if (
            current is None
            or event["ingestion_timestamp"] > current["ingestion_timestamp"]
            or (
                event["ingestion_timestamp"] == current["ingestion_timestamp"]
                and event["event_timestamp"] > current["event_timestamp"]
            )
        ):
            latest_by_event_id[event_id] = event

    return list(latest_by_event_id.values())


def write_partitioned_curated(events: list[dict]) -> set[str]:
    grouped = {}

    for event in events:
        event_date = event["event_date"]
        grouped.setdefault(event_date, []).append(event)

    affected_partitions = set(grouped.keys())

    for event_date, rows in grouped.items():
        rows.sort(key=lambda x: x["event_timestamp"])

        partition_dir = CURATED_DIR / f"event_date={event_date}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        output_file = partition_dir / "events.jsonl"
        # We overwrite each event_date partition to keep writes idempotent during reprocessing.
        with output_file.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")

    return affected_partitions


def main() -> None:
    raw_events = load_raw_events()
    flat_events = [flatten_event(event) for event in raw_events]

    valid_events = [event for event in flat_events if is_valid_event(event)]
    invalid_count = len(flat_events) - len(valid_events)

    curated_events = deduplicate_events(valid_events)
    affected_partitions = write_partitioned_curated(curated_events)

    print(f"Loaded raw rows: {len(raw_events)}")
    print(f"Flattened rows: {len(flat_events)}")
    print(f"Rejected invalid rows: {invalid_count}")
    print(f"Valid rows before dedup: {len(valid_events)}")
    print(f"Curated rows after dedup: {len(curated_events)}")
    print(f"Dedup removed: {len(valid_events) - len(curated_events)} rows")
    print(f"Reprocessing partitions: {sorted(affected_partitions)}")
    print(f"Wrote curated partitions to: {CURATED_DIR}")


if __name__ == "__main__":
    main()
