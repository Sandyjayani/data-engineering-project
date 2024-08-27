# placeholder setup a scheduler for step function 

resource "aws_cloudwatch_event_rule" "scheduler" {
  name = "trigger-step-function"
  description = "trigger step function every 15 minutes"
  schedule_expression = "rate(15 minutes)"
}

resource "aws_cloudwatch_event_target" "step_function_target" {
  rule      = aws_cloudwatch_event_rule.scheduler.name
  arn       = aws_sfn_state_machine.unified_state_machine.arn
  role_arn  = aws_iam_role.unified_state_machine_role.arn
}

# resource "aws_lambda_permission" "allow_eventbridge" {
#   statement_id  = "AllowExecutionFromEventBridge"
#   action        = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.quote_handler.function_name
#   principal     = "events.amazonaws.com"
#   source_arn    = aws_cloudwatch_event_rule.scheduler.arn
# }