import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


EVENT_TYPES = ["page_view", "add_to_cart", "purchase"]
CATEGORIES = ["books", "electronics", "home", "beauty", "toys"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-file", default="data/raw/events.jsonl")
    parser.add_argument("--event-count", type=int, default=20)
    parser.add_argument("--duplicate-ratio", type=float, default=0.1)
    parser.add_argument("--late-ratio", type=float, default=0.1)
    parser.add_argument("--v2-ratio", type=float, default=0.5)
    return parser.parse_args()


def generate_event(i: int, late_ratio: float, v2_ratio: float) -> dict:
    now = datetime.now(timezone.utc)
    event_time = now - timedelta(minutes=random.randint(0, 500))
    event_type = random.choice(EVENT_TYPES)
    price = round(random.uniform(10, 300), 2)

    if random.random() < late_ratio:
        ingestion_time = event_time + timedelta(days=random.randint(1, 2), minutes=random.randint(5, 60))
    else:
        ingestion_time = event_time + timedelta(minutes=random.randint(0, 30))

    event_version = "v2" if random.random() < v2_ratio else "v1"

    event = {
        "event_id": f"evt_{i}",
        "event_type": event_type,
        "event_timestamp": event_time.isoformat(),
        "ingestion_timestamp": ingestion_time.isoformat(),
        "event_version": event_version,
        "user_id": f"user_{random.randint(1, 10)}",
        "session_id": f"sess_{random.randint(1, 5)}",
        "country": "PL",
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

    if event_version == "v2":
        event["device_type"] = random.choice(DEVICE_TYPES)

    return event


def main() -> None:
    args = parse_args()
    output_path = Path(args.output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    events = []
    for i in range(args.event_count):
        event = generate_event(i, args.late_ratio, args.v2_ratio)
        events.append(event)

        if random.random() < args.duplicate_ratio:
            duplicate = dict(event)
            duplicate["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
            events.append(duplicate)

    with output_path.open("w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")

    print(f"Wrote {len(events)} rows to {output_path}")


if __name__ == "__main__":
    main()
