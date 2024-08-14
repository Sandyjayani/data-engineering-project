# lambda role
resource "aws_iam_role" "lambda_role" {
    name_prefix = "role-${var.lambda_name}"
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
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

#TODO:
# bucket permission doc
data "aws_iam_policy_document" "s3_document" {
    statement {

        actions = [
            "s3:GetObject",
            "s3:ListBucket",
            "s3:PutObject"
        ] # what permissions do we need? put object? more?

        resources = [
            "${aws_s3_bucket.ingestion_bucket.arn}/*",
            "${aws_s3_bucket.lambda_code_bucket.arn}/*",
        ]
    }
}

# cloudwatch permission doc
data "aws_iam_policy_document" "cw_document" {
    statement {

        actions = [ "logs:CreateLogGroup" ]

        resources = [
            "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
        ]
    }

    statement {

        actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

        resources = [
            "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.lambda_name}:*"
        ]
    }
}


# policies
resource "aws_iam_policy" "s3_policy" {
    name_prefix = "s3-policy-${var.lambda_name}"
    policy = data.aws_iam_policy_document.s3_document.json
}

resource "aws_iam_policy" "cw_policy" {
    name_prefix = "cw-policy-${var.lambda_name}"
    policy = data.aws_iam_policy_document.cw_document.json
}


# policy attachments
resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
    role = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
    role = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.cw_policy.arn
}