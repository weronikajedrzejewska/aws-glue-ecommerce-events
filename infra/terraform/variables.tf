variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "eu-central-1"
}

variable "project" {
  description = "Project name used as a prefix for resource names"
  type        = string
  default     = "ecommerce-events"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}
