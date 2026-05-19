variable "aws_region" {
  description = "AWS region (ca-central-1 for PIPEDA)"
  type        = string
  default     = "ca-central-1"
}

variable "s3_bucket_name" {
  description = "S3 bucket for bronze layer"
  type        = string
  default     = "voiceops-bronze"
}

variable "db_username" {
  description = "RDS master username"
  type        = string
  default     = "voiceops"
}

variable "db_password" {
  description = "RDS master password"
  type        = string
  sensitive   = true
}

variable "my_ip_cidr" {
  description = "Your public IP in CIDR notation (e.g. 1.2.3.4/32)"
  type        = string
}
