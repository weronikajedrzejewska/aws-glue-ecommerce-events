import os
from dataclasses import dataclass


def build_s3_uri(bucket: str, prefix: str) -> str:
    bucket_name = bucket.strip()
    object_prefix = prefix.strip("/")
    return f"s3://{bucket_name}/{object_prefix}"


def get_env_or_default(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


@dataclass(frozen=True)
class GluePaths:
    raw_path: str
    curated_path: str
    analytics_path: str


def resolve_s3_path(
    *,
    explicit_uri_env: str,
    bucket_env: str,
    prefix_env: str,
    default_prefix: str,
) -> str:
    explicit_uri = os.getenv(explicit_uri_env)
    if explicit_uri:
        return explicit_uri.strip().rstrip("/")

    bucket = os.getenv(bucket_env)
    if not bucket:
        raise ValueError(
            f"Missing required configuration. Set {explicit_uri_env} or {bucket_env}."
        )

    prefix = get_env_or_default(prefix_env, default_prefix)
    return build_s3_uri(bucket, prefix)


def get_glue_paths() -> GluePaths:
    curated_bucket = os.getenv("S3_CURATED_BUCKET") or os.getenv("S3_RAW_BUCKET")

    raw_path = resolve_s3_path(
        explicit_uri_env="RAW_S3_URI",
        bucket_env="S3_RAW_BUCKET",
        prefix_env="RAW_PREFIX",
        default_prefix="raw/events",
    )
    curated_path = resolve_s3_path(
        explicit_uri_env="CURATED_S3_URI",
        bucket_env="S3_CURATED_BUCKET" if os.getenv("S3_CURATED_BUCKET") else "S3_RAW_BUCKET",
        prefix_env="CURATED_PREFIX",
        default_prefix="curated/events",
    )
    analytics_path = resolve_s3_path(
        explicit_uri_env="ANALYTICS_S3_URI",
        bucket_env=(
            "S3_ANALYTICS_BUCKET"
            if os.getenv("S3_ANALYTICS_BUCKET")
            else ("S3_CURATED_BUCKET" if curated_bucket else "S3_RAW_BUCKET")
        ),
        prefix_env="ANALYTICS_PREFIX",
        default_prefix="analytics/abandoned_carts",
    )

    return GluePaths(
        raw_path=raw_path,
        curated_path=curated_path,
        analytics_path=analytics_path,
    )
