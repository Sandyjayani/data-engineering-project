resource "aws_cloudwatch_log_metric_filter" "transformation_critial_error_filter" {
  name           = "CriticalErrorFilter-Transformation"
  pattern        = "CRITICAL"
  log_group_name = "/aws/lambda/${var.transformation_lambda_name}"

  metric_transformation {
    name      = "CriticalErrorCount"
    namespace = "CriticalErrorCount-Transformation"
    value     = "1"
  }
}


resource "aws_cloudwatch_metric_alarm" "critical_error_alarm" {
  alarm_name                = "critical_error_alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = aws_cloudwatch_log_metric_filter.transformation_critial_error_filter.metric_transformation[0].name
  namespace                 = aws_cloudwatch_log_metric_filter.transformation_critial_error_filter.metric_transformation[0].namespace
  period                    = 60
  statistic                 = "Sum"
  threshold                 = 1
  alarm_description         = "Trigger alarm when a [CRITICAL] is logged"
  alarm_actions             = [var.critical_error_topic_arn]
  ok_actions                = [var.critical_error_topic_arn]
  insufficient_data_actions = [var.critical_error_topic_arn]
}