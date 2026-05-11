resource "aws_glue_job" "raw_to_curated" {
  name     = "${local.prefix}-raw-to-curated"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.raw.bucket}/scripts/glue_curate_events.py"
    python_version  = "3"
  }

  default_arguments = {
    "--S3_RAW_BUCKET"                = aws_s3_bucket.raw.bucket
    "--S3_CURATED_BUCKET"            = aws_s3_bucket.curated.bucket
    "--job-language"                 = "python"
    "--enable-metrics"               = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-job-insights"          = "true"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 60
}

resource "aws_glue_job" "curated_to_abandoned_carts" {
  name     = "${local.prefix}-curated-to-abandoned-carts"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.raw.bucket}/scripts/glue_build_abandoned_carts.py"
    python_version  = "3"
  }

  default_arguments = {
    "--S3_CURATED_BUCKET"                = aws_s3_bucket.curated.bucket
    "--S3_ANALYTICS_BUCKET"              = aws_s3_bucket.analytics.bucket
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-job-insights"              = "true"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 60
}

resource "aws_glue_crawler" "curated" {
  name          = "${local.prefix}-curated-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.ecommerce.name

  s3_target {
    path = "s3://${aws_s3_bucket.curated.bucket}/curated/events"
  }
}

resource "aws_glue_crawler" "analytics" {
  name          = "${local.prefix}-analytics-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.ecommerce.name

  s3_target {
    path = "s3://${aws_s3_bucket.analytics.bucket}/analytics/abandoned_carts"
  }
}

resource "aws_glue_catalog_database" "ecommerce" {
  name = replace("${local.prefix}-db", "-", "_")
}

resource "aws_athena_workgroup" "ecommerce" {
  name = "${local.prefix}-workgroup"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/query-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
