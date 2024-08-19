resource "aws_sns_topic" "critical_error_topic_extraction" {
  name = "critical_error_notifications"

  lifecycle {
      prevent_destroy = true
    }
}


resource "aws_sns_topic_subscription" "email_subscription_extraction" {
  topic_arn = aws_sns_topic.critical_error_topic_extraction.arn
  protocol  = "email"
  endpoint  = var.subscription_email
  
  lifecycle {
      prevent_destroy = true
    }
}

output "critical_error_topic_extraction_arn" {
    value = aws_sns_topic.critical_error_topic_extraction.arn
}
