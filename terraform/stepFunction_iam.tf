resource "aws_iam_role" "unified_state_machine_role" {
    name_prefix        = "unified_state_machine-"
    assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com",
                        "states.amazonaws.com",
                        "events.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}


data "aws_iam_policy_document" "sf_document" {
  statement {
    actions = [
      "lambda:InvokeFunction",
      "states:StartExecution"
      ]

    resources =  [
      aws_iam_role.unified_state_machine_role.arn,
      "arn:aws:states:*:*:stateMachine:*"
    ]
  }
}

data "aws_iam_policy_document" "lm_document" {
  statement {
    actions = [
      "lambda:InvokeFunction",
      "states:StartExecution"
    ]

    resources = [
      module.extraction.lambda_function_extraction_arn,
      "arn:aws:lambda:eu-west-2:*"
    ]
  }
}

resource "aws_iam_policy" "sf_policy" {
  name_prefix = "sf-policy-unified-"
  policy      = data.aws_iam_policy_document.sf_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_sf_policy_attachment" {
  role       = aws_iam_role.unified_state_machine_role.name
  policy_arn = aws_iam_policy.sf_policy.arn
}

resource "aws_iam_policy" "lm_policy" {
  name_prefix = "lm-policy-currency-lambda-"
  policy      = data.aws_iam_policy_document.lm_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_lm_policy_attachment" {
  role       = aws_iam_role.unified_state_machine_role.name
  policy_arn = aws_iam_policy.lm_policy.arn
}