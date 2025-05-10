module "ebs_optimizer_lambda" {
  source = "github.com/CostNorm/mcp_tool_iac_template"

  # Basic Lambda settings from variables
  function_name       = var.function_name
  lambda_handler      = var.lambda_handler
  lambda_runtime      = var.lambda_runtime
  lambda_architecture = var.lambda_architecture
  lambda_timeout      = var.lambda_timeout
  lambda_memory       = var.lambda_memory

  # Attach necessary policies for EBS Optimization
  #attach_ebs_policy        = true
  attach_ec2_policy        = true
  attach_cloudwatch_policy = true
  profile = "costnorm"
}
