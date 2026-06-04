resource "aws_sns_topic" "pipeline_alerts" {
  name = "${local.prefix}-pipeline-alerts"
}

# Glue job failure alarms — one per job
resource "aws_cloudwatch_metric_alarm" "glue_raw_to_curated_failure" {
  alarm_name          = "${local.prefix}-raw-to-curated-failure"
  alarm_description   = "Triggers when the raw-to-curated Glue job fails."
  namespace           = "Glue"
  metric_name         = "glue.driver.aggregate.numFailedTask"
  dimensions = {
    JobName = aws_glue_job.raw_to_curated.name
  }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "glue_curated_to_abandoned_failure" {
  alarm_name          = "${local.prefix}-curated-to-abandoned-failure"
  alarm_description   = "Triggers when the curated-to-abandoned-carts Glue job fails."
  namespace           = "Glue"
  metric_name         = "glue.driver.aggregate.numFailedTask"
  dimensions = {
    JobName = aws_glue_job.curated_to_abandoned_carts.name
  }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]
}

# Metric filter on the raw-to-curated Glue job log group to extract rejection rate.
# The Glue job logs a line like: "Rejected (invalid):       12 (15.3%)"
# The filter pattern captures any log line containing a percentage above the threshold.
resource "aws_cloudwatch_log_group" "raw_to_curated" {
  name              = "/aws-glue/jobs/${aws_glue_job.raw_to_curated.name}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_metric_filter" "high_rejection_rate" {
  name           = "${local.prefix}-high-rejection-rate"
  log_group_name = aws_cloudwatch_log_group.raw_to_curated.name
  pattern        = "\"Rejection rate\" \"exceeds 10%\""

  metric_transformation {
    name      = "HighRejectionRateCount"
    namespace = "${local.prefix}/GlueQuality"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "high_rejection_rate" {
  alarm_name          = "${local.prefix}-high-rejection-rate"
  alarm_description   = "Triggers when the raw-to-curated job logs a rejection rate above 10%. The job also raises a ValueError in this case, but the alarm provides an independent signal."
  namespace           = "${local.prefix}/GlueQuality"
  metric_name         = "HighRejectionRateCount"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]
}
