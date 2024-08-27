resource "aws_sfn_state_machine" "unified_state_machine" {
  name     = "unified-state-machine"
  role_arn = aws_iam_role.unified_state_machine_role.arn
  definition = <<EOF
  {
    "Comment": "A unified state machine for the entire ETL process",
    "StartAt": "Extract",
    "States": {
      "Extract": {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "OutputPath": "$.Payload",
        "Parameters": {
          "Payload.$": "$",
          "FunctionName": "${module.extraction.lambda_function_extraction_arn}"
        },
        "Retry": [
          {
            "ErrorEquals": [
              "States.ALL"
            ],
            "MaxAttempts": 3,
            "BackoffRate": 1.5,
            "IntervalSeconds": 1
          }
        ],
        "Catch": [
          {
            "ErrorEquals": [
              "States.ALL"
            ],
            "Next": "Pass"
          }
        ],
        "Next": "New Data Choice",
        "OutputPath": "$.Payload"
      },
      "New Data Choice": {
        "Type": "Choice",
        "Choices": [
          {
            "Variable": "$.body",
            "StringMatches": "*no new data ingested",
            "Next": "Pass"
          }
        ],
        "Default": "Transform"
      },
      "Pass": {
        "Type": "Pass",
        "End": true
      },
      "Transform": {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "OutputPath": "$.Payload",
        "Parameters": {
          "Payload.$": "$",
          "FunctionName": "${module.transformation.lambda_function_transformation_arn}"
        },
        "Retry": [
          {
            "ErrorEquals": [
              "Lambda.ServiceException",
              "Lambda.AWSLambdaException",
              "Lambda.SdkClientException",
              "Lambda.TooManyRequestsException"
            ],
            "IntervalSeconds": 1,
            "MaxAttempts": 3,
            "BackoffRate": 2
          }
        ],
        "Catch": [
          {
            "ErrorEquals": [
              "States.ALL"
            ],
            "ResultPath": null,
            "Next": "Pass"
          }
        ],
        "End": true
      }
    }
  }
  EOF
}
