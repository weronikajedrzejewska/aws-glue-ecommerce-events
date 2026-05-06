import json
from datetime import datetime
from pathlib import Path

RAW_DIR = Path("data/raw/events")
CURATED_DIR = Path("data/curated/events")


def parse_event_date(event_timestamp: str | None) -> str | None:
    if event_timestamp is None:
        return None

    try:
        event_ts = datetime.fromisoformat(event_timestamp)
    except ValueError:
        return None

    return event_ts.date().isoformat()


def load_raw_events(raw_dir: Path = RAW_DIR) -> list[dict]:
    events = []
    for file_path in raw_dir.rglob("*.jsonl"):
        with file_path.open("r", encoding="utf-8") as f:
            for line in f:
                events.append(json.loads(line))
    return events


def flatten_event(event: dict) -> dict:
    payload = event.get("payload", {})

    return {
        "event_id": event.get("event_id"),
        "event_type": event.get("event_type"),
        "event_timestamp": event.get("event_timestamp"),
        "ingestion_timestamp": event.get("ingestion_timestamp"),
        "event_date": parse_event_date(event.get("event_timestamp")),
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
    if event["event_date"] is None:
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


def write_partitioned_curated(events: list[dict], curated_dir: Path = CURATED_DIR) -> set[str]:
    grouped = {}

    for event in events:
        event_date = event["event_date"]
        grouped.setdefault(event_date, []).append(event)

    affected_partitions = set(grouped.keys())

    for event_date, rows in grouped.items():
        rows.sort(key=lambda x: x["event_timestamp"])

        partition_dir = curated_dir / f"event_date={event_date}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        output_file = partition_dir / "events.jsonl"
        # We overwrite each event_date partition to keep writes idempotent during reprocessing.
        with output_file.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")

    return affected_partitions


def run_curated_pipeline(
    raw_dir: Path = RAW_DIR,
    curated_dir: Path = CURATED_DIR,
) -> dict[str, object]:
    raw_events = load_raw_events(raw_dir)
    flat_events = [flatten_event(event) for event in raw_events]

    valid_events = [event for event in flat_events if is_valid_event(event)]
    invalid_count = len(flat_events) - len(valid_events)

    curated_events = deduplicate_events(valid_events)
    affected_partitions = write_partitioned_curated(curated_events, curated_dir)

    return {
        "raw_rows": len(raw_events),
        "flattened_rows": len(flat_events),
        "invalid_rows": invalid_count,
        "valid_rows_before_dedup": len(valid_events),
        "curated_rows": len(curated_events),
        "dedup_removed_rows": len(valid_events) - len(curated_events),
        "affected_partitions": sorted(affected_partitions),
        "output_dir": curated_dir,
    }


def main() -> None:
    stats = run_curated_pipeline()

    print(f"Loaded raw rows: {stats['raw_rows']}")
    print(f"Flattened rows: {stats['flattened_rows']}")
    print(f"Rejected invalid rows: {stats['invalid_rows']}")
    print(f"Valid rows before dedup: {stats['valid_rows_before_dedup']}")
    print(f"Curated rows after dedup: {stats['curated_rows']}")
    print(f"Dedup removed: {stats['dedup_removed_rows']} rows")
    print(f"Reprocessing partitions: {stats['affected_partitions']}")
    print(f"Wrote curated partitions to: {stats['output_dir']}")


if __name__ == "__main__":
    main()
