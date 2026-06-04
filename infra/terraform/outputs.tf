output "raw_bucket" {
  value = aws_s3_bucket.raw.bucket
}

output "curated_bucket" {
  value = aws_s3_bucket.curated.bucket
}

output "analytics_bucket" {
  value = aws_s3_bucket.analytics.bucket
}

output "scripts_bucket" {
  value = aws_s3_bucket.scripts.bucket
}

output "glue_role_arn" {
  value = aws_iam_role.glue.arn
}

output "glue_database_name" {
  value = aws_glue_catalog_database.ecommerce.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.ecommerce.name
}

output "athena_results_bucket" {
  value = aws_s3_bucket.athena_results.bucket
}

output "quarantine_bucket" {
  value = aws_s3_bucket.quarantine.bucket
}
