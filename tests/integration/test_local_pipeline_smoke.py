import json
from pathlib import Path

from src.glue_jobs.curate_events import run_curated_pipeline
from src.transform.build_abandoned_carts import run_abandoned_carts_pipeline


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file_handle:
        for row in rows:
            file_handle.write(json.dumps(row) + "\n")


def test_local_pipeline_smoke(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw" / "events"
    curated_dir = tmp_path / "curated" / "events"
    output_file = tmp_path / "analytics" / "abandoned_carts.jsonl"

    raw_rows = [
        {
            "event_id": "evt-1",
            "event_type": "add_to_cart",
            "event_timestamp": "2026-04-20T10:00:00+00:00",
            "ingestion_timestamp": "2026-04-20T10:01:00+00:00",
            "event_version": "v2",
            "user_id": "user-1",
            "session_id": "sess-1",
            "country": "PL",
            "event_source": "web",
            "device_type": "mobile",
            "payload": {
                "product_id": "prod-1",
                "category": "books",
                "price": 25.0,
                "currency": "USD",
                "quantity": 1,
                "cart_value": 25.0,
            },
        },
        {
            "event_id": "evt-1",
            "event_type": "add_to_cart",
            "event_timestamp": "2026-04-20T10:00:30+00:00",
            "ingestion_timestamp": "2026-04-20T10:02:00+00:00",
            "event_version": "v2",
            "user_id": "user-1",
            "session_id": "sess-1",
            "country": "PL",
            "event_source": "web",
            "device_type": "mobile",
            "payload": {
                "product_id": "prod-1",
                "category": "books",
                "price": 30.0,
                "currency": "USD",
                "quantity": 1,
                "cart_value": 30.0,
            },
        },
        {
            "event_id": "evt-2",
            "event_type": "purchase",
            "event_timestamp": "2026-04-20T10:10:00+00:00",
            "ingestion_timestamp": "2026-04-20T10:11:00+00:00",
            "event_version": "v1",
            "user_id": "user-1",
            "session_id": "sess-1",
            "country": "PL",
            "event_source": "web",
            "payload": {
                "product_id": "prod-1",
                "category": "books",
                "price": 30.0,
                "currency": "USD",
                "quantity": 1,
                "cart_value": 30.0,
            },
        },
    ]

    write_jsonl(
        raw_dir / "event_date=2026-04-20" / "hour=10" / "events_000.jsonl",
        raw_rows,
    )

    curate_stats = run_curated_pipeline(raw_dir=raw_dir, curated_dir=curated_dir)
    analytics_stats = run_abandoned_carts_pipeline(curated_dir=curated_dir, output_file=output_file)

    assert curate_stats["curated_rows"] == 2
    assert curate_stats["dedup_removed_rows"] == 1
    assert analytics_stats["abandoned_carts_rows"] == 1
    assert output_file.exists()
