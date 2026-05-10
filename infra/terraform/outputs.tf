output "raw_bucket" {
  value = aws_s3_bucket.raw.bucket
}

output "curated_bucket" {
  value = aws_s3_bucket.curated.bucket
}

output "analytics_bucket" {
  value = aws_s3_bucket.analytics.bucket
}

output "glue_role_arn" {
  value = aws_iam_role.glue.arn
}
