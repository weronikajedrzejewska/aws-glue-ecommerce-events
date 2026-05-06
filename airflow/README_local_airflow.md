# Local Airflow Demo (Docker)

This setup is for a one-time portfolio demo:
- run Airflow UI locally
- trigger AWS Glue + Crawler DAG
- capture screenshots
- shut everything down

## Prerequisites

- Docker Desktop running
- AWS credentials available in `${HOME}/.aws`
- AWS resources already created in `eu-central-1`:
  - Glue job: `raw-to-curated`
  - Glue job: `curated-to-abandoned-carts`
  - Glue crawler: `curated-events-crawler`
  - Glue crawler: `abandoned-carts-crawler`

## Start Airflow

```bash
cd airflow
mkdir -p logs plugins
docker compose up -d
docker logs -f airflow-standalone
```

If DAGs are not visible after startup on macOS, copy DAG files into the container:

```bash
docker cp ./dags/ecommerce_pipeline_dag.py airflow-standalone:/opt/airflow/dags/ecommerce_pipeline_dag.py
docker exec airflow-standalone airflow dags list
```

In logs, copy credentials shown by Airflow standalone (`username`/`password`).

UI:

`http://localhost:8080`

## Run Pipeline DAG

1. Open DAG `aws_ecommerce_glue_pipeline`.
2. Unpause DAG.
3. Click `Trigger DAG`.
4. Wait for all tasks to turn `success`.

## Suggested Screenshots

1. DAG list with `aws_ecommerce_glue_pipeline`.
2. DAG details showing schedule.
3. Graph view with all tasks.
4. Successful DAG run.
5. Task log of Glue operator.

## Stop Demo Environment

```bash
cd airflow
docker compose down
```
