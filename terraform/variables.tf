variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  default     = "asia-south1"
}

variable "bucket_name" {
  description = "GCS bucket name"
}

variable "dataset" {
  description = "BigQuery dataset name"
}