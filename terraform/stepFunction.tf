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

# # placeholder
# resource "aws_sfn_state_machine" "extraction_state_machine" {
#   name     = "my-state-machine"
#   role_arn = aws_iam_role.lambda_role.arn
#   definition = <<EOF
#   {
#   "Comment": "A description of my state machine",
#   "StartAt": "Extract",
#   "States": {
#     "Extract": {
#       "Type": "Task",
#       "Resource": "arn:aws:states:::lambda:invoke",
#       "OutputPath": "$.Payload",
#       "Parameters": {
#         "Payload.$": "$",
#         "FunctionName": "arn:aws:lambda:eu-west-2:975050178788:function:extract:$LATEST"
#       },
#       "Retry": [
#         {
#           "ErrorEquals": [
#             "Lambda.ServiceException",
#             "Lambda.AWSLambdaException",
#             "Lambda.SdkClientException",
#             "Lambda.TooManyRequestsException"
#           ],
#           "IntervalSeconds": 1,
#           "MaxAttempts": 3,
#           "BackoffRate": 2
#         }
#       ],
#       "Next": "Transform"
#     },
#     "Transform": {
#       "Type": "Task",
#       "Resource": "arn:aws:states:::lambda:invoke",
#       "OutputPath": "$.Payload",
#       "Parameters": {
#         "Payload.$": "$",
#         "FunctionName": "arn:aws:lambda:eu-west-2:975050178788:function:transform:$LATEST"
#       },
#       "Retry": [
#         {
#           "ErrorEquals": [
#             "Lambda.ServiceException",
#             "Lambda.AWSLambdaException",
#             "Lambda.SdkClientException",
#             "Lambda.TooManyRequestsException"
#           ],
#           "IntervalSeconds": 1,
#           "MaxAttempts": 3,
#           "BackoffRate": 2
#         }
#       ],
#       "Next": "Load"
#     },
#     "Load": {
#       "Type": "Task",
#       "Resource": "arn:aws:states:::lambda:invoke",
#       "OutputPath": "$.Payload",
#       "Parameters": {
#         "Payload.$": "$",
#         "FunctionName": "arn:aws:lambda:eu-west-2:975050178788:function:load:$LATEST"
#       },
#       "Retry": [
#         {
#           "ErrorEquals": [
#             "Lambda.ServiceException",
#             "Lambda.AWSLambdaException",
#             "Lambda.SdkClientException",
#             "Lambda.TooManyRequestsException"
#           ],
#           "IntervalSeconds": 1,
#           "MaxAttempts": 3,
#           "BackoffRate": 2
#         }
#       ],
#       "End": true
#     }
#   }
# }
# EOF
# }



# # {
# #   "Comment": "A description of my state machine",
# #   "StartAt": "Lambda Invoke",
# #   "States": {
# #     "Lambda Invoke": {
# #       "Type": "Task",
# #       "Resource": "arn:aws:states:::lambda:invoke",
# #       "OutputPath": "$.Payload",
# #       "Parameters": {
# #         "Payload.$": "$",
# #         "FunctionName": "arn:aws:lambda:eu-west-2:637423603039:function:extraction_lambda_handler:$LATEST"
# #       },
# #       "Retry": [
# #         {
# #           "ErrorEquals": [
# #             "States.ALL"
# #           ],
# #           "MaxAttempts": 3,
# #           "BackoffRate": 1.5,
# #           "IntervalSeconds": 1
# #         }
# #       ],
# #       "End": true,
# #       "Catch": [
# #         {
# #           "ErrorEquals": [
# #             "States.ALL"
# #           ],
# #           "Next": "SNS Publish",
# #           "ResultPath": null
# #         }
# #       ]
# #     },
# #     "SNS Publish": {
# #       "Type": "Task",
# #       "Resource": "arn:aws:states:::sns:publish",
# #       "Parameters": {
# #         "TopicArn": "arn:aws:sns:eu-west-2:637423603039:ExtractLambdaFailure",
# #         "Message.$": "$"
# #       },
# #       "End": true
# #     }
# #   }
# # }