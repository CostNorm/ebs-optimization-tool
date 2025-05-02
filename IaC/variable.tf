variable "region" {
    description = "AWS region for deploying resources"
    type        = string
    default     = "ap-northeast-2"
}
variable "profile" {
    description = "AWS profile name to use for authentication"
    type        = string
    default     = "costnorm"
}
variable "function_name" {
  description = "Name of the EBS Optimizer Lambda function"
  type        = string
  default     = "ebs-optimizer-lambda"
}
variable "lambda_timeout" {
  description = "Maximum execution time for the Lambda function (seconds)"
  type        = number
  default     = 300
}
variable "lambda_memory" {
  description = "Memory allocated to the Lambda function (MB)"
  type        = number
  default     = 1024
}
variable "lambda_runtime" {
  description = "Lambda function runtime environment"
  type        = string
  default     = "python3.13"
}
variable "lambda_handler" {
  description = "Lambda function handler (filename.handler_function)"
  type        = string
  default     = "lambda_function.lambda_handler"
}
variable "lambda_architecture" {
  description = "Lambda function instruction set architecture"
  type        = string
  default     = "x86_64"
} 