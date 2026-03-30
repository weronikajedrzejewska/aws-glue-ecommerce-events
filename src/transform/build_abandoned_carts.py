import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path


CURATED_DIR = Path("data/curated/events")
OUTPUT_FILE = Path("data/analytics/abandoned_carts.jsonl")


def parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def load_curated_events() -> list[dict]:
    events = []
    for file_path in CURATED_DIR.rglob("*.jsonl"):
        with file_path.open("r", encoding="utf-8") as f:
            for line in f:
                events.append(json.loads(line))
    return events


def build_abandoned_carts(events: list[dict]) -> list[dict]:
    grouped = defaultdict(list)

    for event in events:
        if event["event_type"] not in {"add_to_cart", "purchase"}:
            continue

        # Grain: one record per user/session/product, regardless of date boundary crossings.
        key = (
            event["user_id"],
            event["session_id"],
            event["product_id"],
        )
        grouped[key].append(event)

    results = []

    for key, rows in grouped.items():
        user_id, session_id, product_id = key

        add_to_cart_events = [r for r in rows if r["event_type"] == "add_to_cart"]
        purchase_events = [r for r in rows if r["event_type"] == "purchase"]

        if not add_to_cart_events:
            continue

        add_to_cart_events.sort(key=lambda x: parse_ts(x["event_timestamp"]))
        added_to_cart_ts = add_to_cart_events[0]["event_timestamp"]

        # Only purchases after add_to_cart should close the cart.
        purchase_events = [
            r for r in purchase_events
            if parse_ts(r["event_timestamp"]) >= parse_ts(added_to_cart_ts)
        ]
        purchase_events.sort(key=lambda x: parse_ts(x["event_timestamp"]))

        purchased_ts = purchase_events[0]["event_timestamp"] if purchase_events else None
        event_date = added_to_cart_ts[:10]

        # Max cart_value is used here as a simple proxy for final cart size.
        cart_values = [r["cart_value"] for r in rows if r["cart_value"] is not None]
        cart_value = max(cart_values) if cart_values else None

        time_to_purchase_minutes = None
        if purchased_ts is not None:
            start = parse_ts(added_to_cart_ts)
            end = parse_ts(purchased_ts)
            time_to_purchase_minutes = round((end - start).total_seconds() / 60, 2)

        results.append(
            {
                "event_date": event_date,
                "user_id": user_id,
                "session_id": session_id,
                "product_id": product_id,
                "added_to_cart_ts": added_to_cart_ts,
                "purchased_ts": purchased_ts,
                "cart_value": cart_value,
                "abandoned_cart_flag": 0 if purchased_ts else 1,
                "time_to_purchase_minutes": time_to_purchase_minutes,
                "has_multiple_adds": len(add_to_cart_events) > 1,
            }
        )
    results.sort(key=lambda x: (x["event_date"], x["user_id"], x["session_id"], x["product_id"]))
    return results


def main() -> None:
    events = load_curated_events()
    abandoned_carts = build_abandoned_carts(events)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        for row in abandoned_carts:
            f.write(json.dumps(row) + "\n")

    print(f"Loaded curated rows: {len(events)}")
    print(f"Wrote abandoned carts rows: {len(abandoned_carts)}")
    print(f"Output file: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
