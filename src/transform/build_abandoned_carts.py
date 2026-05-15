import json
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

CURATED_DIR = Path("data/curated/events")
OUTPUT_FILE = Path("data/analytics/abandoned_carts.jsonl")


def parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def load_curated_events(curated_dir: Path = CURATED_DIR) -> list[dict]:
    events = []
    for file_path in curated_dir.rglob("*.jsonl"):
        with file_path.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    logger.warning("Skipping malformed JSON line in %s", file_path)
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
            r
            for r in purchase_events
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


def write_abandoned_carts(rows: list[dict], output_file: Path = OUTPUT_FILE) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def run_abandoned_carts_pipeline(
    curated_dir: Path = CURATED_DIR,
    output_file: Path = OUTPUT_FILE,
) -> dict[str, object]:
    events = load_curated_events(curated_dir)
    abandoned_carts = build_abandoned_carts(events)
    write_abandoned_carts(abandoned_carts, output_file)

    return {
        "curated_rows": len(events),
        "abandoned_carts_rows": len(abandoned_carts),
        "output_file": output_file,
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stats = run_abandoned_carts_pipeline()

    logger.info("Loaded curated rows: %d", stats["curated_rows"])
    logger.info("Wrote abandoned carts rows: %d", stats["abandoned_carts_rows"])
    logger.info("Output file: %s", stats["output_file"])


if __name__ == "__main__":
    main()
