terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ============================================================
# S3 — Bronze Layer
# ============================================================
resource "aws_s3_bucket" "bronze" {
  bucket = var.s3_bucket_name

  tags = {
    Project     = "voiceops"
    Layer       = "bronze"
    Environment = "demo"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ============================================================
# RDS — Silver Layer
# ============================================================

# Use default VPC
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# Security group: allow Postgres from your IP
resource "aws_security_group" "rds" {
  name        = "voiceops-rds-sg"
  description = "Allow Postgres access for VoiceOps"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [var.my_ip_cidr]
    description = "Postgres from my IP"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Project = "voiceops"
  }
}

resource "aws_db_subnet_group" "voiceops" {
  name       = "voiceops-subnet-group"
  subnet_ids = data.aws_subnets.default.ids

  tags = {
    Project = "voiceops"
  }
}

resource "aws_db_instance" "silver" {
  identifier     = "voiceops-silver"
  engine         = "postgres"
  engine_version = "16"
  instance_class = "db.t4g.micro"

  allocated_storage = 20
  storage_type      = "gp3"
  storage_encrypted = true

  db_name  = "voiceops"
  username = var.db_username
  password = var.db_password

  vpc_security_group_ids = [aws_security_group.rds.id]
  db_subnet_group_name   = aws_db_subnet_group.voiceops.name
  publicly_accessible    = true

  skip_final_snapshot = true
  deletion_protection = false

  backup_retention_period = 0

  tags = {
    Project     = "voiceops"
    Layer       = "silver"
    Environment = "demo"
  }
}
