# AWS Event-Driven E-commerce Pipeline

Local-first, AWS-shaped data engineering project that simulates messy e-commerce event data and processes it into curated and analytics-ready datasets.

The project is designed around real event pipeline problems such as:
- duplicates
- late-arriving data
- schema evolution
- bad data
- small files
- incremental-style reprocessing

## Business Goal

Build an e-commerce event pipeline that supports downstream analytics, with a focus on the `abandoned_carts` use case.

The final analytics layer answers questions such as:
- how many carts were abandoned per day
- how many carts were converted into purchases
- what was the cart value
- how long it took users to convert after adding items to cart

## Tech Stack

- Python
- PySpark
- JSONL
- local file-based raw / curated / analytics layers
- Amazon S3
- AWS Glue
- AWS Glue Crawlers
- AWS Glue Data Catalog
- Amazon Athena
- GitHub Actions

## Architecture

```mermaid
flowchart LR
    A["Synthetic Event Generator"] --> B["Raw Events<br/>duplicates, late data, schema evolution, bad data"]
    B --> C["Curated Events<br/>flatten, validate, deduplicate"]
    C --> D["Abandoned Carts Analytics<br/>session-product grain"]

    B --> E["Raw Storage<br/>event_date=YYYY-MM-DD / hour=HH"]
    C --> F["Curated Storage<br/>event_date=YYYY-MM-DD"]
    D --> G["Analytics Storage<br/>event_date=YYYY-MM-DD"]

    E -. "AWS equivalent" .-> H["Amazon S3"]
    F -. "AWS equivalent" .-> I["AWS Glue + S3"]
    G -. "AWS equivalent" .-> J["Athena + Glue Catalog"]
```

## Repository Structure

```text
src/generator/generate_events.py
src/glue_jobs/curate_events.py
src/glue_jobs/glue_curate_events.py
src/glue_jobs/glue_build_abandoned_carts.py
src/transform/build_abandoned_carts.py
src/utils/aws_paths.py
tests/unit/
tests/integration/
.github/workflows/ci.yml
```

### Raw Layer

Synthetic event data is generated into a raw zone partitioned by ingestion time.

Example layout:

```text
data/raw/events/event_date=YYYY-MM-DD/hour=HH/
```

Characteristics:
- append-like raw ingestion
- many small files
- duplicates
- late-arriving events
- schema evolution (`v1`, `v2`)
- bad data injected intentionally for validation logic

### Curated Layer

Raw events are flattened, validated, deduplicated, and written into curated partitions by event date.

Example layout:

```text
data/curated/events/event_date=YYYY-MM-DD/events.jsonl
data/curated_spark/events/event_date=YYYY-MM-DD/part-*.json
```

Curated processing includes:
- flattening nested `payload`
- filtering invalid rows
- deduplication by `event_id`
- `last-write-wins` based on `ingestion_timestamp`
- `event_timestamp` tie-breaker for edge cases
- partition-level reprocessing by `event_date`

### Analytics Layer

The final analytics output is an `abandoned_carts` dataset.

Example layout:

```text
data/analytics/abandoned_carts.jsonl
data/analytics_spark/abandoned_carts/event_date=YYYY-MM-DD/part-*.json
```

## Event Model

Each raw event contains:
- `event_id`
- `event_type`
- `event_timestamp`
- `ingestion_timestamp`
- `event_version`
- `user_id`
- `session_id`
- `country`
- `event_source`
- optional `device_type`
- nested `payload`

Payload fields:
- `product_id`
- `category`
- `price`
- `currency`
- `quantity`
- `cart_value`

## Data Engineering Scenarios Covered

### Duplicates

The generator creates duplicate events with the same `event_id`, but duplicates may contain slightly different values and later ingestion timestamps.

Dedup strategy:
- partition by `event_id`
- keep the latest record by `ingestion_timestamp`
- use `event_timestamp` as a tie-breaker

### Late Data

A share of events is intentionally ingested 1-2 days late, and some arrive 3-7 days late.

This is used to simulate:
- late-arriving event handling
- reprocessing of affected curated partitions

### Schema Evolution

The generator emits:
- `v1` events without `device_type`
- `v2` events with `device_type`

This simulates schema changes over time.

### Bad Data

A small share of events contains invalid values such as:
- `price = null`
- `country = null`

Curated processing filters invalid records for business-critical event types.

### Session Realism

The generator creates simple event sequences such as:
- `page_view`
- `page_view -> add_to_cart`
- `page_view -> add_to_cart -> purchase`

This makes downstream abandoned cart analytics realistic.

## Pipeline Steps

### 1. Generate raw events

Script:

```text
src/generator/generate_events.py
```

Generates raw event files partitioned by ingestion time.

### 2. Curate raw events in plain Python

Script:

```text
src/glue_jobs/curate_events.py
```

Responsibilities:
- load raw JSONL files
- flatten payload
- validate records
- deduplicate events
- write curated partitions by `event_date`

### 3. Curate raw events in PySpark / Glue-style

Script:

```text
src/glue_jobs/glue_curate_events.py
```

Responsibilities:
- Spark-based curated processing
- partitioned output by `event_date`
- dynamic partition overwrite
- Glue-style transformation logic

### 4. Build abandoned carts in plain Python

Script:

```text
src/transform/build_abandoned_carts.py
```

Responsibilities:
- group events by `user_id`, `session_id`, `product_id`
- match `add_to_cart` with `purchase`
- compute abandonment flag and time to purchase

### 5. Build abandoned carts in PySpark / Glue-style

Script:

```text
src/glue_jobs/glue_build_abandoned_carts.py
```

Responsibilities:
- Spark-based analytics output
- partitioned output by `event_date`
- abandoned cart metrics at session-product grain

### 6. Orchestrate pipeline in Airflow (local demo)

Files:

```text
airflow/dags/ecommerce_pipeline_dag.py
airflow/docker-compose.yml
airflow/README_local_airflow.md
```

Responsibilities:
- trigger Glue jobs and crawlers in the required order
- show scheduler and DAG-run behavior for portfolio screenshots
- provide a one-time online-like orchestration demo without permanent hosting

## What Makes It Production-Style

- messy event simulation instead of clean tutorial data
- deterministic deduplication with `last-write-wins`
- event-date partition reprocessing for late data
- separate local Python and Glue-style Spark implementations
- configurable Glue S3 paths through environment variables instead of hardcoded buckets
- unit tests and smoke test for the local pipeline
- CI workflow for linting and test execution
- structured quality summary logged per run: rejection breakdown by category, dedup rate, conversion split — making anomalies visible without querying data directly

## Local Quality Checks

The repository now includes a lightweight quality gate for portfolio and CI use:

- `ruff` for Python linting
- `pytest` unit tests for generator, curated processing, and abandoned cart logic
- smoke test covering `raw -> curated -> analytics` locally without AWS services

Run locally:

```bash
python3 -m venv .venv
.venv/bin/python3.11 -m pip install -r requirements-dev.txt
make lint
make test
make smoke-test
```

## Abandoned Carts Output

Grain:
- one row per `user_id`, `session_id`, `product_id`

Fields:
- `event_date`
- `user_id`
- `session_id`
- `product_id`
- `added_to_cart_ts`
- `purchased_ts`
- `cart_value`
- `abandoned_cart_flag`
- `time_to_purchase_minutes`

## Example Run

Generate data:

```bash
python src/generator/generate_events.py --session-count 30 --days 5 --duplicate-ratio 0.2 --late-ratio 0.2 --v2-ratio 0.5 --files-per-hour 2
python src/generator/generate_events.py --session-count 10 --days 1 --duplicate-ratio 0.2 --late-ratio 0.3
```

Curate:

```bash
python src/glue_jobs/curate_events.py
python src/glue_jobs/glue_curate_events.py
```

Build analytics:

```bash
python src/transform/build_abandoned_carts.py
python src/glue_jobs/glue_build_abandoned_carts.py
```

Run quality checks:

```bash
make lint
make test
make smoke-test
```

## Current Output Example

Spark analytics output currently produces:
- abandoned carts
- purchased carts
- non-null `time_to_purchase_minutes`
- multiple event-date partitions

This confirms that the pipeline supports both abandoned and converted session flows.

## Pipeline Quality Summary

Each Glue job logs a structured quality breakdown to CloudWatch at the end of every run.

Example output from `glue_curate_events`:

```
=== glue_curate_events quality summary ===
Raw rows read:            12450
Rejected (invalid):       187 (1.5%)
  - missing event_id/ts:  42
  - bad price (cart/buy): 145
Duplicates removed:       934 (7.6%)
Curated rows written:     11329
Partitions written:       5
Output path:              s3://.../curated/events
```

Example output from `glue_build_abandoned_carts`:

```
=== glue_build_abandoned_carts quality summary ===
Curated input rows:       11329
  - add_to_cart events:   5201
  - purchase events:      3847
Output rows:              5201
  - abandoned:            2108 (40.5%)
  - converted:            3093 (59.5%)
Partitions written:       5
Output path:              s3://.../analytics/abandoned_carts
```

This breakdown makes it straightforward to detect unexpected rejection spikes, deduplication anomalies, or conversion rate drift between pipeline runs — without querying the data directly.

## AWS Deployment (Verified)

The pipeline was deployed and validated on AWS using:

- Amazon S3 for raw, curated, and analytics storage
- AWS Glue jobs for Spark-based transformations
- AWS Glue Crawlers for cataloging partitioned datasets
- AWS Glue Data Catalog for table metadata
- Amazon Athena for querying curated and analytics outputs

Validated flow:
- raw JSONL files uploaded to S3
- `raw-to-curated` Glue job executed successfully
- curated partitions registered through Glue Crawler
- `curated-to-abandoned-carts` Glue job executed successfully
- analytics partitions registered through Glue Crawler
- Athena queries returned expected results for both curated and abandoned cart datasets

### AWS Screenshots

Glue jobs registered in the console:

![Glue jobs](docs/images/glue_jobs.png)

Glue job run confirmation:

![Glue job run](docs/images/glue_job_run.png)

Glue Data Catalog after crawler run:

![Glue catalog](docs/images/glue_catalog.png)

Athena — conversion rate by day:

![Athena conversion rate](docs/images/athena_conversion_rate.png)

Athena — lost cart value by day:

![Athena lost cart value](docs/images/athena_lost_cart_value.png)

Athena — average time to purchase:

![Athena avg time to purchase](docs/images/athena_avg_time_to_purchase.png)

Airflow DAG graph view (local Docker demo):

![Airflow DAG graph](docs/images/airflow_graph.png)

## AWS Mapping

This project was first implemented locally and then validated on AWS.

AWS services used in the deployed version:
- Raw zone -> Amazon S3
- Curated and analytics transformations -> AWS Glue
- Table discovery -> AWS Glue Crawlers + Glue Data Catalog
- Query layer -> Amazon Athena

Target S3 layout:

```text
s3://<bucket>/raw/events/event_date=YYYY-MM-DD/hour=HH/
s3://<bucket>/curated/events/event_date=YYYY-MM-DD/
s3://<bucket>/analytics/abandoned_carts/event_date=YYYY-MM-DD/
```

Glue jobs read their locations from environment variables, so the repo can be reused across AWS accounts without editing source files.

Supported configuration:

```bash
export S3_RAW_BUCKET=...
export S3_CURATED_BUCKET=...
export S3_ANALYTICS_BUCKET=...
export RAW_PREFIX=raw/events
export CURATED_PREFIX=curated/events
export ANALYTICS_PREFIX=analytics/abandoned_carts
```

Optional explicit URI overrides:

```bash
export RAW_S3_URI=s3://...
export CURATED_S3_URI=s3://...
export ANALYTICS_S3_URI=s3://...
```

## Incremental Processing Strategy

The local project currently reprocesses datasets from local files, but the intended AWS strategy is:

- raw ingestion is append-only
- new raw partitions are detected by ingestion time
- affected `event_date` partitions are identified from incoming records
- only impacted curated and analytics partitions are rewritten

## Design Decisions and Trade-offs

**Incremental processing:** The Glue jobs use dynamic partition overwrite, which rewrites only affected `event_date` partitions. Full incremental checkpointing (tracking exactly which raw files have been processed) is not implemented. In production this would be handled by a state store (e.g. DynamoDB) or Glue job bookmarks, but adds operational complexity not warranted for this pipeline's data volume.

**Airflow hosting:** The DAG runs locally via Docker Compose for demo purposes. In production, MWAA or a self-managed Airflow cluster would be used to hold live AWS credentials and run on a schedule. The DAG code is written against the same `GlueJobOperator` and `GlueCrawlerOperator` that MWAA would use, so the migration path is configuration-only.

**Terraform scope:** The Terraform configuration provisions all AWS resources used in this project (S3 buckets, Glue jobs, crawlers, Glue Catalog database, IAM role, Athena workgroup). It does not manage uploading the Glue scripts to S3, which is handled separately as a deployment step before running `terraform apply`.

**IAM permissions:** The Glue IAM role uses `AWSGlueServiceRole` plus a scoped inline policy limited to the three project S3 buckets and the scripts bucket. Cross-account or VPC-level restrictions are out of scope for this project.

## Why This Project Matters

This project is meant to demonstrate practical data engineering thinking, not just file movement.

It shows:
- event data modeling
- realistic messy data simulation
- deduplication strategy
- late-data handling
- schema evolution
- data quality filtering
- curated layer design
- business-facing analytics output
- a migration path from local PySpark to AWS Glue and Athena
- testability, CI, and configuration hygiene expected in production-leaning projects
