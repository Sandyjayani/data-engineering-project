# lambda role
resource "aws_iam_role" "transformation_lambda_role" {
    name_prefix = "role-${var.transformation_lambda_name}"
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


data "aws_iam_policy_document" "s3_document" {
    statement {

        actions = [
            "s3:GetObject",
            "s3:ListBucket",
            "s3:PutObject"
        ] 

        resources = [
            "${var.s3_transformation_bucket_arn}/*",
            "${var.s3_lambda_code_bucket_arn}/*",
            "${var.s3_transformation_bucket_arn}",
            "${var.s3_lambda_code_bucket_arn}",
            "${var.s3_ingestion_bucket_arn}/*",
            "${var.s3_ingestion_bucket_arn}"

        ]
    }
}

data "aws_iam_policy_document" "cw_document" {
    statement {

        actions = [ "logs:CreateLogGroup" ]

        resources = [
            "arn:aws:logs:${var.region}:${var.account_id}:*"
        ]
    }

    statement {

        actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

        resources = [
            "arn:aws:logs:${var.region}:${var.account_id}:log-group:/aws/lambda/${var.transformation_lambda_name}:*"
        ]
    }
}

data "aws_iam_policy_document" "secretsmanager_document" {
    statement {

        actions = [
            "secretsmanager:GetSecretValue"
        ] 
 
        resources = [
            var.warehouse_secrets_arn
        ]
    }
}


# policies
resource "aws_iam_policy" "s3_policy" {
    name_prefix = "s3-policy-${var.transformation_lambda_name}"
    policy = data.aws_iam_policy_document.s3_document.json
}

resource "aws_iam_policy" "cw_policy" {
    name_prefix = "cw-policy-${var.transformation_lambda_name}"
    policy = data.aws_iam_policy_document.cw_document.json
}

resource "aws_iam_policy" "secretsmanager_policy" {
    name_prefix = "secretsmanager-policy-${var.transformation_lambda_name}"
    policy = data.aws_iam_policy_document.secretsmanager_document.json
}


# policy attachments
resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
    role = aws_iam_role.transformation_lambda_role.name
    policy_arn = aws_iam_policy.s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
    role = aws_iam_role.transformation_lambda_role.name
    policy_arn = aws_iam_policy.cw_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_secretsmanager_policy_attachment" {
    role = aws_iam_role.transformation_lambda_role.name
    policy_arn = aws_iam_policy.secretsmanager_policy.arn
}