resource "aws_s3_bucket" "dlq_tracker" {
  bucket = "${var.bucket_name}-${var.environment}"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "dlq_tracker_versioning" {
  bucket = aws_s3_bucket.dlq_tracker.id
  versioning_configuration {
    status = "Enabled"
  }
}

output "bucket_name" {
  value = aws_s3_bucket.dlq_tracker.bucket
}