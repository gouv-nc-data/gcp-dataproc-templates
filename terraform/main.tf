locals {
  parent_folder_id            = "658965356947" # production folder
  postgresl_driver_remote_url = "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.6/postgresql-42.2.6.jar"
}

module "project-factory" {
  source                      = "terraform-google-modules/project-factory/google"
  version                     = "~> 14.4"
  name                        = "prj-dinum-data-templates"
  org_id                      = var.org_id
  billing_account             = var.billing_account
  group_name                  = "data"
  random_project_id           = true
  budget_alert_spent_percents = [50, 75, 90]
  budget_amount               = 10
  create_project_sa           = false
  default_service_account     = "delete"
  folder_id                   = "folders/${local.parent_folder_id}"
  labels = {
    direction = var.direction
  }
  activate_apis = [
    "storage.googleapis.com",
  ]
}

resource "google_storage_bucket" "bucket" {
  project                     = module.project-factory.project_id
  name                        = "bucket-${module.project-factory.project_id}"
  location                    = var.region
  storage_class               = "REGIONAL"
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }
}

# ----------------------------------------------------
# Ressources nécessitant une custom image
# ----------------------------------------------------
resource "google_artifact_registry_repository" "ar_repo_templates" {
  project       = module.project-factory.project_id
  repository_id = "templates"
  description   = "docker repository pour les images templates du GNC"
  format        = "DOCKER"
  # cleanup_policies {
  #   id     = "keep-minimum-versions"
  #   action = "KEEP"
  #   most_recent_versions {
  #     keep_count            = 5
  #   }
}

resource "google_service_account" "service_account" {
  account_id   = "sa-artifact-registry"
  display_name = "Service Account created by terraform for artifact registry write access"
  project      = module.project-factory.project_id
}

resource "google_project_iam_member" "service_account_bindings" {
  project = module.project-factory.project_id
  role    = "roles/artifactregistry.admin"
  member  = "serviceAccount:${google_service_account.service_account.email}"
}

#---------------------------------------------------------
# fonction oracle2bq
#---------------------------------------------------------
module "bigquery-dataset-dev" {
  source     = "git::https://github.com/GoogleCloudPlatform/cloud-foundation-fabric//modules/bigquery-dataset?ref=v26.0.0"
  project_id = module.dinum-exp-datawarehouse.project_id
  id         = "dataset_dev"
  location   = "EU"
}

module "oracle2bq-function" {
  source           = "git::https://github.com/gouv-nc-data/gcp-function.git//?ref=feature/add_vpn_option"
  project_id       = module.dinum-exp-datawarehouse.project_id
  project_name     = "oracle2bq"
  group_name       = local.dinum_exp_group_name
  region           = var.region
  # ingress_settings = "all"
  schedule         = "15 4 1 12 *" # “At 04:15 on day-of-month 1 in december.”
  # env = {
  #   "JIRA_TOKEN" : data.google_secret_manager_secret_version.jira-bq-key-secret.secret_data
  # }
  direction             = local.dinum_exp_name
  notification_channels = module.dinum-exp-datawarehouse.notification_channels
  create_bucket         = false # le true par défaut bloque la montée de version du provider
  enable_vpn            = true
}
