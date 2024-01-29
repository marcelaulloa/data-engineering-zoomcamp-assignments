variable "project" {
  description = "Project"
  default     = "plucky-cascade-305020"
}

variable "region" {
    description = "Project Region"
    default = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My Big Query Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "plucky-cascade-305020"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}