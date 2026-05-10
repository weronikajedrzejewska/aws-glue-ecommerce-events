import os
from dataclasses import dataclass


@dataclass(frozen=True)
class GluePaths:
    raw_path: str
    curated_path: str
    analytics_path: str


def _get_path(explicit_uri_env: str, bucket_env: str, default_prefix: str) -> str:
    if uri := os.getenv(explicit_uri_env):
        return uri.strip().rstrip("/")
    bucket = os.getenv(bucket_env, "").strip()
    if not bucket:
        raise ValueError(f"Set {explicit_uri_env} or {bucket_env}.")
    prefix = os.getenv(f"{bucket_env.replace('BUCKET', 'PREFIX')}", default_prefix).strip("/")
    return f"s3://{bucket}/{prefix}"


def get_glue_paths() -> GluePaths:
    return GluePaths(
        raw_path=_get_path("RAW_S3_URI", "S3_RAW_BUCKET", "raw/events"),
        curated_path=_get_path("CURATED_S3_URI", "S3_CURATED_BUCKET", "curated/events"),
        analytics_path=_get_path(
            "ANALYTICS_S3_URI", "S3_ANALYTICS_BUCKET", "analytics/abandoned_carts"
        ),
    )
