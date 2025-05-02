provider "aws" {
  region  = var.region
  profile = var.profile
}

module "ebs_optimizer_lambda" {
  source = "github.com/CostNorm/mcp_tool_iac_template"

  # Basic Lambda settings from variables
  function_name       = var.function_name
  lambda_handler      = var.lambda_handler
  lambda_runtime      = var.lambda_runtime
  lambda_architecture = var.lambda_architecture
  lambda_timeout      = var.lambda_timeout
  lambda_memory       = var.lambda_memory

  # AWS provider settings
  region  = var.region
  profile = var.profile

  # Attach necessary policies for EBS Optimization
  attach_ebs_policy        = true # Assumed parameter for EBS permissions
  attach_ec2_policy        = true # Assumed parameter for EC2 permissions (like describe instances)
  attach_cloudwatch_policy = true # Assumed parameter for CloudWatch permissions (like get metrics)

  # Specify the path to the Lambda code within this project
  lambda_code_path = "../code"
}

# Output the Lambda function name (optional, but useful)
output "ebs_optimizer_lambda_function_name" {
  description = "The name of the deployed EBS Optimizer Lambda function"
  value       = module.ebs_optimizer_lambda.lambda_function_name # Assumes the module outputs the function name
}

output "ebs_optimizer_lambda_function_arn" {
  description = "The ARN of the deployed EBS Optimizer Lambda function"
  value       = module.ebs_optimizer_lambda.lambda_function_arn # Assumes the module outputs the function ARN
}

output "ebs_optimizer_lambda_iam_role_name" {
  description = "The name of the IAM role created for the EBS Optimizer Lambda function"
  value       = module.ebs_optimizer_lambda.lambda_iam_role_name # Assumes the module outputs the role name
}