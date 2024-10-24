locals {
  data_lake_bucket = "de_data_lake"
}

variable "project" {
  description = "Your GCP Project ID"
   default = "red-splice-439613-q8"
}

variable "region" {
  description = "Region for GCP resources."
  default = "southamerica-east1"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "bq_dataset" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "de_dataset"
}

variable "credentials" {
    description = "The credentials to access GCP"
    type = string
    sensitive = true
    default = "google_credentials.json"
}
 