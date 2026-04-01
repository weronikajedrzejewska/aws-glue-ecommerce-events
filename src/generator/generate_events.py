import argparse
import json
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4


CATEGORIES = ["books", "electronics", "home", "beauty", "toys"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
COUNTRIES = ["PL", "DE", "FR", None]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data/raw/events")
    parser.add_argument("--session-count", type=int, default=20)
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--duplicate-ratio", type=float, default=0.1)
    parser.add_argument("--late-ratio", type=float, default=0.1)
    parser.add_argument("--v2-ratio", type=float, default=0.5)
    parser.add_argument("--files-per-hour", type=int, default=2)
    return parser.parse_args()


def build_base_event(
    event_type: str,
    session_id: str,
    user_id: str,
    product_id: str,
    category: str,
    price: float | None,
    event_time: datetime,
    late_ratio: float,
    v2_ratio: float,
) -> dict:
    # Some events are intentionally ingested late to simulate late-arriving data.
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
        "user_id": user_id,
        "session_id": session_id,
        "country": random.choice(COUNTRIES),
        "event_source": "web",
        "payload": {
            "product_id": product_id,
            "category": category,
            "price": price,
            "currency": "USD",
            "quantity": 1,
            "cart_value": price,
        },
    }

    # v2 adds device_type to simulate schema evolution.
    if event_version == "v2":
        event["device_type"] = random.choice(DEVICE_TYPES)

    # A small share of events intentionally contains bad data for validation tests.
    if random.random() < 0.05:
        event["payload"]["price"] = None
        event["payload"]["cart_value"] = None

    return event


def generate_session_events(days: int, late_ratio: float, v2_ratio: float) -> list[dict]:
    now = datetime.now(timezone.utc)
    session_start = now - timedelta(
        days=random.randint(0, max(days - 1, 0)),
        minutes=random.randint(0, 1439),
    )

    user_id = f"user_{random.randint(1, 10)}"
    session_id = f"sess_{uuid4().hex[:8]}"
    product_id = f"prod_{random.randint(100, 200)}"
    category = random.choice(CATEGORIES)
    price = round(random.uniform(10, 300), 2)

    # Session patterns create more realistic event sequences for analytics use cases.
    session_pattern = random.choices(
        ["view_only", "cart_only", "cart_purchase"],
        weights=[0.2, 0.4, 0.4],
    )[0]

    events = [
        build_base_event(
            event_type="page_view",
            session_id=session_id,
            user_id=user_id,
            product_id=product_id,
            category=category,
            price=price,
            event_time=session_start,
            late_ratio=late_ratio,
            v2_ratio=v2_ratio,
        )
    ]

    if session_pattern in {"cart_only", "cart_purchase"}:
        add_to_cart_time = session_start + timedelta(minutes=random.randint(1, 20))
        events.append(
            build_base_event(
                event_type="add_to_cart",
                session_id=session_id,
                user_id=user_id,
                product_id=product_id,
                category=category,
                price=price,
                event_time=add_to_cart_time,
                late_ratio=late_ratio,
                v2_ratio=v2_ratio,
            )
        )

        # Some sessions add the same product multiple times to create edge cases.
        if random.random() < 0.2:
            second_add_time = add_to_cart_time + timedelta(minutes=random.randint(1, 10))
            events.append(
                build_base_event(
                    event_type="add_to_cart",
                    session_id=session_id,
                    user_id=user_id,
                    product_id=product_id,
                    category=category,
                    price=price,
                    event_time=second_add_time,
                    late_ratio=late_ratio,
                    v2_ratio=v2_ratio,
                )
            )

    if session_pattern == "cart_purchase":
        purchase_base = [e for e in events if e["event_type"] == "add_to_cart"][0]
        purchase_time = datetime.fromisoformat(purchase_base["event_timestamp"]) + timedelta(
            minutes=random.randint(1, 60)
        )
        events.append(
            build_base_event(
                event_type="purchase",
                session_id=session_id,
                user_id=user_id,
                product_id=product_id,
                category=category,
                price=price,
                event_time=purchase_time,
                late_ratio=late_ratio,
                v2_ratio=v2_ratio,
            )
        )

    return events


def make_dirty_duplicate(event: dict) -> dict:
    duplicate = {
        **event,
        "payload": dict(event["payload"]),
    }

    # Duplicates keep the same event_id but may arrive later with slightly different values.
    duplicate["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()

    if duplicate["payload"]["price"] is not None:
        duplicate["payload"]["price"] = round(
            duplicate["payload"]["price"] + random.uniform(-5, 5), 2
        )
        duplicate["payload"]["cart_value"] = duplicate["payload"]["price"]

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

    # Each run writes new file names so the raw zone behaves like append-only ingestion.
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    for (date_part, hour_part), rows in grouped.items():
        partition_path = base_path / f"event_date={date_part}" / f"hour={hour_part}"
        partition_path.mkdir(parents=True, exist_ok=True)

        # Small files are intentional to simulate a common raw-zone problem on S3.
        chunk_size = max(1, math.ceil(len(rows) / files_per_hour))
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            file_path = partition_path / f"events_{run_id}_{i // chunk_size:03d}.jsonl"
            with file_path.open("w", encoding="utf-8") as f:
                for row in chunk:
                    f.write(json.dumps(row) + "\n")


def main() -> None:
    random.seed(42)
    args = parse_args()
    events = []

    for _ in range(args.session_count):
        session_events = generate_session_events(args.days, args.late_ratio, args.v2_ratio)
        events.extend(session_events)

        for event in session_events:
            if random.random() < args.duplicate_ratio:
                events.append(make_dirty_duplicate(event))

    write_partitioned_files(events, args.output_dir, args.files_per_hour)
    print(f"Wrote {len(events)} rows to partitioned raw files in {args.output_dir}")


if __name__ == "__main__":
    main()
