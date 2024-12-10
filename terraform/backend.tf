terraform {
  backend "gcs" {
    bucket = "bkt-dinum-c-basic-mgr-tfstate"
    prefix = "terraform/postgresql-to-bigquery-template" # prefix initial, est ce qu'on peut le changer après couop ss pb ?
  }
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~>5.41"
    }
  }
}
