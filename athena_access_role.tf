resource "aws_iam_role" "athena_access_role" {
  name = "AthenaAccessRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "athena.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy" "athena_access_policy" {
  name        = "AthenaAccessPolicy"
  description = "Policy to allow Athena access"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults"
        ],
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "athena_access_attachment" {
  role       = aws_iam_role.athena_access_role.name
  policy_arn = aws_iam_policy.athena_access_policy.arn
}
