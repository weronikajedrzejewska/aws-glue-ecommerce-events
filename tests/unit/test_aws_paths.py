import pytest

from src.utils.aws_paths import get_glue_paths


def test_get_glue_paths_builds_uris_from_buckets(monkeypatch):
    monkeypatch.setenv("S3_RAW_BUCKET", "my-raw-bucket")
    monkeypatch.setenv("S3_CURATED_BUCKET", "my-curated-bucket")
    monkeypatch.setenv("S3_ANALYTICS_BUCKET", "my-analytics-bucket")

    paths = get_glue_paths()

    assert paths.raw_path == "s3://my-raw-bucket/raw/events"
    assert paths.curated_path == "s3://my-curated-bucket/curated/events"
    assert paths.analytics_path == "s3://my-analytics-bucket/analytics/abandoned_carts"


def test_get_glue_paths_explicit_uri_takes_precedence(monkeypatch):
    monkeypatch.setenv("RAW_S3_URI", "s3://override-bucket/custom/path")
    monkeypatch.setenv("S3_RAW_BUCKET", "should-be-ignored")
    monkeypatch.setenv("S3_CURATED_BUCKET", "my-curated-bucket")
    monkeypatch.setenv("S3_ANALYTICS_BUCKET", "my-analytics-bucket")

    paths = get_glue_paths()

    assert paths.raw_path == "s3://override-bucket/custom/path"


def test_get_glue_paths_raises_when_bucket_missing(monkeypatch):
    monkeypatch.delenv("S3_RAW_BUCKET", raising=False)
    monkeypatch.delenv("RAW_S3_URI", raising=False)
    monkeypatch.setenv("S3_CURATED_BUCKET", "my-curated-bucket")
    monkeypatch.setenv("S3_ANALYTICS_BUCKET", "my-analytics-bucket")

    with pytest.raises(ValueError, match="S3_RAW_BUCKET"):
        get_glue_paths()
