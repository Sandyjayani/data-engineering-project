resource "aws_cloudwatch_event_rule" "extract_step_function_scheduler" {
  name = "trigger-extraction-step-function"
  description = "trigger extraction step function every 15 minutes"
  schedule_expression = "rate(15 minutes)"
}


resource "aws_cloudwatch_event_target" "step_function_target" {
  rule      = aws_cloudwatch_event_rule.extract_step_function_scheduler.name
  arn       = aws_sfn_state_machine.sfn_state_machine.arn
  role_arn  = aws_iam_role.lambda_role.arn
}