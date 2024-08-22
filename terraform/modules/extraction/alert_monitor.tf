# Here we set up filter for CRITICIAL ONLY, might need to discuss
# if we would like to add error/ change some logging message
# from error to critical in the python codes

resource "aws_cloudwatch_log_metric_filter" "extraction_critial_error_filter" {
  name           = "CriticalErrorFilter-Extraction"
  pattern        = "CRITICAL"
  log_group_name = "/aws/lambda/${var.extraction_lambda_name}"

  metric_transformation {
    name      = "CriticalErrorCount"
    namespace = "CriticalErrorCount-Extraction"
    value     = "1"
  }
}

# resource "aws_sns_topic" "critical_error_topic_extraction" {
#   name = "critical_error_notifications"
# }


# resource "aws_sns_topic_subscription" "email_subscription_extraction" {
#   topic_arn = aws_sns_topic.critical_error_topic_extraction.arn
#   protocol  = "email"
#   endpoint  = var.subscription_email
# }

resource "aws_cloudwatch_metric_alarm" "critical_error_alarm" {
  alarm_name                = "critical_error_alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = aws_cloudwatch_log_metric_filter.extraction_critial_error_filter.metric_transformation[0].name
  namespace                 = aws_cloudwatch_log_metric_filter.extraction_critial_error_filter.metric_transformation[0].namespace
  period                    = 60
  statistic                 = "Sum"
  threshold                 = 1
  alarm_description         = "Trigger alarm when a [CRITICAL] is logged"
  alarm_actions             = [var.critical_error_topic_arn]
  ok_actions                = [var.critical_error_topic_arn]
  insufficient_data_actions = [var.critical_error_topic_arn]
}
  