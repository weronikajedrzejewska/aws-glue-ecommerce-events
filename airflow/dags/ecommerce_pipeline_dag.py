"""
Airflow DAG skeleton for orchestrating the AWS ecommerce pipeline.

Flow:
1) raw-to-curated Glue job
2) curated crawler
3) curated-to-abandoned-carts Glue job
4) analytics crawler
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator


AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")
GLUE_JOB_RAW_TO_CURATED = os.getenv("GLUE_JOB_RAW_TO_CURATED", "raw-to-curated")
GLUE_JOB_CURATED_TO_ABANDONED = os.getenv(
    "GLUE_JOB_CURATED_TO_ABANDONED",
    "curated-to-abandoned-carts",
)
GLUE_CRAWLER_CURATED = os.getenv("GLUE_CRAWLER_CURATED", "curated-events-crawler")
GLUE_CRAWLER_ANALYTICS = os.getenv(
    "GLUE_CRAWLER_ANALYTICS",
    "abandoned-carts-crawler",
)


with DAG(
    dag_id="aws_ecommerce_glue_pipeline",
    description="Run Glue jobs and crawlers for ecommerce events pipeline",
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["aws", "glue", "athena", "ecommerce"],
) as dag:
    start = EmptyOperator(task_id="start")

    run_raw_to_curated = GlueJobOperator(
        task_id="run_raw_to_curated_job",
        job_name=GLUE_JOB_RAW_TO_CURATED,
        region_name=AWS_REGION,
        wait_for_completion=True,
        execution_timeout=timedelta(hours=1),
    )

    # In production, crawler frequency can be reduced (for example, scheduled separately).
    crawl_curated = GlueCrawlerOperator(
        task_id="crawl_curated_events",
        config={"Name": GLUE_CRAWLER_CURATED},
        region_name=AWS_REGION,
        wait_for_completion=True,
        execution_timeout=timedelta(hours=1),
    )

    run_curated_to_abandoned = GlueJobOperator(
        task_id="run_curated_to_abandoned_job",
        job_name=GLUE_JOB_CURATED_TO_ABANDONED,
        region_name=AWS_REGION,
        wait_for_completion=True,
        execution_timeout=timedelta(hours=1),
    )

    crawl_analytics = GlueCrawlerOperator(
        task_id="crawl_abandoned_carts",
        config={"Name": GLUE_CRAWLER_ANALYTICS},
        region_name=AWS_REGION,
        wait_for_completion=True,
        execution_timeout=timedelta(hours=1),
    )

    end = EmptyOperator(task_id="end")

    start >> run_raw_to_curated >> crawl_curated >> run_curated_to_abandoned >> crawl_analytics >> end
