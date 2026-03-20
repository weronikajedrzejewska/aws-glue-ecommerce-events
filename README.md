# AWS Event-Driven E-commerce Pipeline

Production-style AWS data engineering project focused on messy event data: duplicates, late-arriving events, schema evolution, small files, and incremental processing.

## Business Goal

Build an e-commerce event pipeline that supports downstream analytics such as:

- funnel conversion (`page_view -> add_to_cart -> purchase`)
- abandoned carts
- session-level behavior analysis

## Scope

This project simulates raw event ingestion and processing on AWS using:

- S3 raw zone
- AWS Glue ETL
- S3 curated zone
- Glue Data Catalog
- Athena analytics
- IAM-based access control

## Source Data

Synthetic e-commerce events generated in Python.

Event types:

- `page_view`
- `add_to_cart`
- `purchase`

The dataset intentionally includes:

- duplicates
- late-arriving events
- out-of-order events
- small raw files
- schema evolution (new fields appear over time)

## Architecture

`Python Event Generator -> S3 Raw -> Glue ETL -> S3 Curated -> Glue Catalog -> Athena`

## Data Layers

### Raw
Raw JSON event files stored in S3.

Characteristics:

- many small files
- duplicates allowed
- late data allowed
- schema can evolve

### Curated
Cleaned and deduplicated events stored in partitioned S3 layout.

Characteristics:

- typed fields
- deduplication by `event_id`
- partitioned by `event_date/hour`
- ready for Athena queries

### Analytics
Athena-facing datasets or queries for business use cases such as:

- conversion funnel
- abandoned carts

## Engineering Focus

This project is designed to demonstrate:

- incremental Glue processing
- rerun-safe deduplication
- late data handling
- partition strategy in S3
- IAM design for Glue/Athena/S3
- AWS-oriented operational thinking

## Definition of Done

- synthetic event generator produces realistic raw data
- raw events land in S3 with partitions
- Glue job processes new data incrementally
- curated layer removes duplicates and handles late arrivals
- Athena can query final business dataset
- README explains architecture and trade-offs
