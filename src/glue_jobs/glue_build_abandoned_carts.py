from pyspark.sql import SparkSession, Window, functions as F


CURATED_PATH = "data/curated_spark/events"
OUTPUT_PATH = "data/analytics_spark/abandoned_carts"
TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"


def get_spark() -> SparkSession:
    spark = SparkSession.builder.appName("glue_build_abandoned_carts").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def main() -> None:
    spark = get_spark()

    # Read curated events written in Spark and partitioned by event_date.
    curated_df = spark.read.json(f"{CURATED_PATH}/event_date=*/*.json")

    carts_df = (
        curated_df
        .filter(F.col("event_type") == "add_to_cart")
        .select(
            "user_id",
            "session_id",
            "product_id",
            F.col("event_timestamp").alias("added_to_cart_ts"),
            "cart_value",
        )
        .withColumn(
            "added_to_cart_ts_parsed",
            F.to_timestamp("added_to_cart_ts", TS_FORMAT),
        )
    )

    purchases_df = (
        curated_df
        .filter(F.col("event_type") == "purchase")
        .select(
            "user_id",
            "session_id",
            "product_id",
            F.col("event_timestamp").alias("purchased_ts"),
        )
        .withColumn(
            "purchased_ts_parsed",
            F.to_timestamp("purchased_ts", TS_FORMAT),
        )
    )

    joined_df = (
        carts_df.alias("c")
        .join(
            purchases_df.alias("p"),
            on=["user_id", "session_id", "product_id"],
            how="left",
        )
        .filter(
            F.col("purchased_ts_parsed").isNull()
            | (F.col("purchased_ts_parsed") >= F.col("added_to_cart_ts_parsed"))
        )
    )

    # Keep the earliest purchase that closes a given cart event.
    w = Window.partitionBy(
        "user_id", "session_id", "product_id", "added_to_cart_ts"
    ).orderBy(F.col("purchased_ts_parsed").asc_nulls_last())

    result_df = (
        joined_df
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn("event_date", F.to_date("added_to_cart_ts_parsed"))
        .withColumn(
            "abandoned_cart_flag",
            F.when(F.col("purchased_ts").isNull(), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "time_to_purchase_minutes",
            F.when(
                F.col("purchased_ts_parsed").isNotNull(),
                (
                    F.col("purchased_ts_parsed").cast("long")
                    - F.col("added_to_cart_ts_parsed").cast("long")
                ) / 60.0,
            ),
        )
        .select(
            "event_date",
            "user_id",
            "session_id",
            "product_id",
            "added_to_cart_ts",
            "purchased_ts",
            "cart_value",
            "abandoned_cart_flag",
            "time_to_purchase_minutes",
        )
    )

    # Counts are used here for observability/debugging.
    curated_count = curated_df.count()
    result_count = result_df.count()
    partitions_written = result_df.select("event_date").distinct().count()

    (
        result_df.write
        .mode("overwrite")
        .partitionBy("event_date")
        .json(OUTPUT_PATH)
    )

    print(f"Curated input rows: {curated_count}")
    print(f"Abandoned carts rows: {result_count}")
    print(f"Partitions written: {partitions_written}")
    print(f"Output path: {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
