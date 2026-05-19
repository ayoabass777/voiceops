output "rds_endpoint" {
  description = "RDS Postgres endpoint (host:port)"
  value       = aws_db_instance.silver.endpoint
}

output "rds_host" {
  description = "RDS hostname (for .env)"
  value       = aws_db_instance.silver.address
}

output "s3_bucket" {
  description = "S3 bronze bucket name"
  value       = aws_s3_bucket.bronze.id
}

output "s3_bucket_arn" {
  description = "S3 bronze bucket ARN"
  value       = aws_s3_bucket.bronze.arn
}

output "connection_summary" {
  description = "Copy these to your .env"
  value = <<-EOT

    ============================================
    VoiceOps AWS Resources — ca-central-1
    ============================================

    # Add to .env:
    PG_HOST=${aws_db_instance.silver.address}
    PG_PORT=5432
    PG_DB=voiceops
    PG_USER=${var.db_username}
    PG_PASSWORD=<your_password>
    S3_BUCKET=${aws_s3_bucket.bronze.id}
    AWS_REGION=${var.aws_region}

    # dbt profiles.yml:
    host: ${aws_db_instance.silver.address}
    port: 5432

    ============================================
  EOT
}
