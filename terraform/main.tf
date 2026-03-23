provider "google" {
  project = var.project_id
  region  = var.region
}

# -------------------------
# GCS Bucket (Data Lake)
# -------------------------
resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
}

# -------------------------
# BigQuery Dataset
# -------------------------
resource "google_bigquery_dataset" "dataset" {
  dataset_id                 = var.dataset
  location                   = var.region
  delete_contents_on_destroy = true
}