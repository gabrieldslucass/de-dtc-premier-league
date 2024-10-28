locals {
  data_lake_bucket = "de_data_lake"
  data_lake_transformed_bucket = "de_data_lake_transformed"
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
  default = "premier_league"
}

variable "credentials" {
    description = "The credentials to access GCP"
    type = string
    sensitive = true
    default = "google_credentials.json"
}
 