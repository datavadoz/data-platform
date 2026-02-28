terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.8.0"
    }
  }

  backend "gcs" {
    bucket = "conda-terraform"
    prefix = "gcp-resources/state"
  }
}

resource "google_folder" "cellphones" {
  display_name = "CellphoneS"
  parent       = "organizations/138569242965"
}

resource "google_project" "conda_cps_dev" {
  name            = "conda-cps-dev"
  project_id      = "conda-cps-dev"
  billing_account = "01DDAB-3D0A6F-F91FAF"
  folder_id       = google_folder.cellphones.name
  deletion_policy = "DELETE"
}

resource "google_project" "conda_cps_prod" {
  name            = "conda-cps-prod"
  project_id      = "conda-cps-prod"
  billing_account = "01DDAB-3D0A6F-F91FAF"
  folder_id       = google_folder.cellphones.name
  deletion_policy = "DELETE"
}

# module "sa_conda_cps_cloudrun_dev" {
#   source        = "terraform-google-modules/service-accounts/google"
#   version       = "4.7"
#   project_id    = google_project.conda_cps_dev.id
#   prefix        = "sa-"
#   names         = ["cloudrun"]
#   project_roles = [
#     "${google_project.conda_cps_dev.id}=>roles/bigquery.dataEditor",
#     "${google_project.conda_cps_dev.id}=>roles/bigquery.jobUser",
#   ]
# }

# module "sa_conda_cps_cloudrun_prod" {
#   source        = "terraform-google-modules/service-accounts/google"
#   version       = "4.7"
#   project_id    = google_project.conda_cps_prod.id
#   prefix        = "sa-"
#   names         = ["cloudrun"]
#   project_roles = [
#     "${google_project.conda_cps_prod.id}=>roles/bigquery.dataEditor",
#     "${google_project.conda_cps_prod.id}=>roles/bigquery.jobUser",
#   ]
# }
