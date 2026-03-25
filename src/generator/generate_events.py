import argparse
import json
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4


EVENT_TYPES = ["page_view", "add_to_cart", "purchase"]
CATEGORIES = ["books", "electronics", "home", "beauty", "toys"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data/raw/events")
    parser.add_argument("--event-count", type=int, default=20)
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--duplicate-ratio", type=float, default=0.1)
    parser.add_argument("--late-ratio", type=float, default=0.1)
    parser.add_argument("--v2-ratio", type=float, default=0.5)
    parser.add_argument("--files-per-hour", type=int, default=2)
    return parser.parse_args()


def generate_event(days: int, late_ratio: float, v2_ratio: float) -> dict:
    now = datetime.now(timezone.utc)
    event_time = now - timedelta(
        days=random.randint(0, max(days - 1, 0)),
        minutes=random.randint(0, 1439),
    )
    event_type = random.choice(EVENT_TYPES)
    price = round(random.uniform(10, 300), 2)

    if random.random() < late_ratio:
        if random.random() < 0.1:
            ingestion_time = event_time + timedelta(days=random.randint(3, 7), minutes=random.randint(5, 60))
        else:
            ingestion_time = event_time + timedelta(days=random.randint(1, 2), minutes=random.randint(5, 60))
    else:
        ingestion_time = event_time + timedelta(minutes=random.randint(0, 30))

    event_version = "v2" if random.random() < v2_ratio else "v1"

    event = {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "event_timestamp": event_time.isoformat(),
        "ingestion_timestamp": ingestion_time.isoformat(),
        "event_version": event_version,
        "user_id": f"user_{random.randint(1, 10)}",
        "session_id": f"sess_{random.randint(1, 5)}",
        "country": random.choice(["PL", "DE", "FR", None]),
        "event_source": "web",
        "payload": {
            "product_id": f"prod_{random.randint(100, 200)}",
            "category": random.choice(CATEGORIES),
            "price": price,
            "currency": "USD",
            "quantity": 1,
            "cart_value": price,
        },
    }

    if random.random() < 0.05:
        event["payload"]["price"] = None

    if event_version == "v2":
        event["device_type"] = random.choice(DEVICE_TYPES)

    return event


def make_dirty_duplicate(event: dict) -> dict:
    duplicate = {
        **event,
        "payload": dict(event["payload"]),
    }

    duplicate["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()

    if duplicate["payload"]["price"] is not None:
        duplicate["payload"]["price"] = round(
            duplicate["payload"]["price"] + random.uniform(-5, 5), 2
        )

    event_ts = datetime.fromisoformat(duplicate["event_timestamp"])
    duplicate["event_timestamp"] = (
        event_ts + timedelta(seconds=random.randint(-30, 30))
    ).isoformat()

    return duplicate


def write_partitioned_files(events: list[dict], output_dir: str, files_per_hour: int) -> None:
    base_path = Path(output_dir)
    base_path.mkdir(parents=True, exist_ok=True)

    grouped = {}
    for event in events:
        ingestion_dt = datetime.fromisoformat(event["ingestion_timestamp"])
        date_part = ingestion_dt.strftime("%Y-%m-%d")
        hour_part = ingestion_dt.strftime("%H")
        key = (date_part, hour_part)
        grouped.setdefault(key, []).append(event)

    for (date_part, hour_part), rows in grouped.items():
        partition_path = base_path / f"event_date={date_part}" / f"hour={hour_part}"
        partition_path.mkdir(parents=True, exist_ok=True)

        chunk_size = max(1, math.ceil(len(rows) / files_per_hour))
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            file_path = partition_path / f"events_{i // chunk_size:03d}.jsonl"
            with file_path.open("w", encoding="utf-8") as f:
                for row in chunk:
                    f.write(json.dumps(row) + "\n")


def main() -> None:
    random.seed(42)
    args = parse_args()
    events = []

    for _ in range(args.event_count):
        event = generate_event(args.days, args.late_ratio, args.v2_ratio)
        events.append(event)

        if random.random() < args.duplicate_ratio:
            events.append(make_dirty_duplicate(event))

    write_partitioned_files(events, args.output_dir, args.files_per_hour)
    print(f"Wrote {len(events)} rows to partitioned raw files in {args.output_dir}")


if __name__ == "__main__":
    main()
