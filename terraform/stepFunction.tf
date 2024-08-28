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
          "IntervalSeconds": 10
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "Step Function Error SNS"
        }
      ],
      "Next": "New Data Choice"
    },
    "Step Function Error SNS": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "Message": {
          "Message": "Error during ETL step function execution",
          "Error Details.$": "States.StringToJson($.Cause)"
        },
        "TopicArn": "${module.permanent.critical_error_topic_arn}"
      },
      "End": true
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
            "States.ALL"
          ],
          "MaxAttempts": 3,
          "BackoffRate": 1.5,
          "IntervalSeconds": 10
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "ResultPath": null,
          "Next": "Step Function Error SNS"
        }
      ],
      "Next": "Load"
    },
    "Load": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "Payload.$": "$",
        "FunctionName": "${module.load.lambda_function_load_arn}"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "IntervalSeconds": 10,
          "MaxAttempts": 3,
          "BackoffRate": 1.5
        }
      ],
      "End": true,
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "Step Function Error SNS"
        }
      ]
    }
  }
}
  EOF
}
