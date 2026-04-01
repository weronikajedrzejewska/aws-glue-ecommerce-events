from pyspark.sql import SparkSession, Window, functions as F


RAW_PATH = "data/raw/events"
CURATED_PATH = "data/curated_spark/events"


def get_spark() -> SparkSession:
    spark = SparkSession.builder.appName("glue_curate_events").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def main() -> None:
    spark = get_spark()

    # Read raw event files from ingestion-time partitions.
    raw_df = spark.read.json(f"{RAW_PATH}/event_date=*/hour=*/*.jsonl")

    # Flatten nested payload fields so downstream analytics can work on a tabular schema.
    flat_df = raw_df.select(
        "event_id",
        "event_type",
        "event_timestamp",
        "ingestion_timestamp",
        "event_version",
        "user_id",
        "session_id",
        "country",
        "event_source",
        "device_type",
        F.col("payload.product_id").alias("product_id"),
        F.col("payload.category").alias("category"),
        F.col("payload.price").alias("price"),
        F.col("payload.currency").alias("currency"),
        F.col("payload.quantity").alias("quantity"),
        F.col("payload.cart_value").alias("cart_value"),
    ).withColumn(
        "event_date",
        F.to_date("event_timestamp")
    )

    # Minimal data quality rules:
    # - event_id and event_timestamp must exist
    # - add_to_cart and purchase events must have a price
    valid_df = flat_df.filter(
        F.col("event_id").isNotNull()
        & F.col("event_timestamp").isNotNull()
        & ~(
            F.col("event_type").isin("add_to_cart", "purchase")
            & F.col("price").isNull()
        )
    )

    # Deduplicate by event_id using last-write-wins on ingestion time.
    # event_timestamp is used as a tiebreaker for edge cases.
    w = Window.partitionBy("event_id").orderBy(
        F.col("ingestion_timestamp").desc(),
        F.col("event_timestamp").desc(),
    )

    curated_df = (
        valid_df
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Counts are used here for observability/debugging.
    # In production, metrics would typically be emitted without repeated full scans.
    raw_count = raw_df.count()
    valid_count = valid_df.count()
    curated_count = curated_df.count()
    partitions_written = curated_df.select("event_date").distinct().count()

    # Dynamic partition overwrite lets us rewrite only affected event_date partitions.
    (
        curated_df.write
        .mode("overwrite")
        .partitionBy("event_date")
        .json(CURATED_PATH)
    )

    print(f"Raw rows: {raw_count}")
    print(f"Valid rows: {valid_count}")
    print(f"Curated rows: {curated_count}")
    print(f"Partitions written: {partitions_written}")
    print(f"Output path: {CURATED_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
