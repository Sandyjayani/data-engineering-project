# placeholder
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