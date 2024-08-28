resource "aws_sns_topic" "critical_error_topic" {
  name = "critical_error_notifications"

  # lifecycle {
  #     prevent_destroy = true
  #   }
}


resource "aws_sns_topic_subscription" "email_subscription" {
  topic_arn = aws_sns_topic.critical_error_topic.arn
  protocol  = "email"
  endpoint  = var.subscription_email
  
  # lifecycle {
  #     prevent_destroy = true
  #   }
}
