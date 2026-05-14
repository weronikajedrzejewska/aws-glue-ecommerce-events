from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from src.utils.aws_paths import get_glue_paths

TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"


def get_spark() -> SparkSession:
    spark = SparkSession.builder.appName("glue_build_abandoned_carts").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def main() -> None:
    spark = get_spark()
    paths = get_glue_paths()

    # Read curated Spark output from S3.
    curated_df = spark.read.json(f"{paths.curated_path}/event_date=*/*.json")

    carts_df = (
        curated_df.filter(F.col("event_type") == "add_to_cart")
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
        curated_df.filter(F.col("event_type") == "purchase")
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
    w = Window.partitionBy("user_id", "session_id", "product_id", "added_to_cart_ts").orderBy(
        F.col("purchased_ts_parsed").asc_nulls_last()
    )

    result_df = (
        joined_df.withColumn("rn", F.row_number().over(w))
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
                )
                / 60.0,
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

    cart_events = curated_df.filter(F.col("event_type") == "add_to_cart").count()
    purchase_events = curated_df.filter(F.col("event_type") == "purchase").count()
    curated_count = curated_df.count()
    result_count = result_df.count()
    abandoned_count = result_df.filter(F.col("abandoned_cart_flag") == 1).count()
    converted_count = result_df.filter(F.col("abandoned_cart_flag") == 0).count()
    partitions_written = result_df.select("event_date").distinct().count()

    (result_df.write.mode("overwrite").partitionBy("event_date").json(paths.analytics_path))

    print("=== glue_build_abandoned_carts quality summary ===")
    print(f"Curated input rows:       {curated_count}")
    print(f"  - add_to_cart events:   {cart_events}")
    print(f"  - purchase events:      {purchase_events}")
    print(f"Output rows:              {result_count}")
    abandoned_pct = 100 * abandoned_count / max(result_count, 1)
    converted_pct = 100 * converted_count / max(result_count, 1)
    print(f"  - abandoned:            {abandoned_count} ({abandoned_pct:.1f}%)")
    print(f"  - converted:            {converted_count} ({converted_pct:.1f}%)")
    print(f"Partitions written:       {partitions_written}")
    print(f"Output path:              {paths.analytics_path}")

    if result_count == 0:
        raise ValueError("Analytics output is empty — no abandoned cart records produced.")

    if converted_pct > 95.0:
        raise ValueError(
            f"Conversion rate {converted_pct:.1f}% is suspiciously high "
            "— purchase events may be double-counted."
        )

    spark.stop()


if __name__ == "__main__":
    main()
