terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = " 7.21.0 "
    }

    google-beta = {
      source  = "hashicorp/google-beta"
      version = " 7.21.0 "
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

# module "sa_conda_cps_cloudrun_dev" {
#   source        = "terraform-google-modules/service-accounts/google"
#   version       = "4.7"
#   project_id    = google_project.conda_cps_dev.project_id
#   prefix        = "sa"
#   names         = ["cloudrun"]
#   project_roles = [
#     "${google_project.conda_cps_dev.project_id}=>roles/bigquery.dataEditor",
#     "${google_project.conda_cps_dev.project_id}=>roles/bigquery.jobUser",
#   ]
# }

# module "sa_conda_cps_cloudrun_prod" {
#   source        = "terraform-google-modules/service-accounts/google"
#   version       = "4.7"
#   project_id    = google_project.conda_cps_prod.project_id
#   prefix        = "sa"
#   names         = ["cloudrun"]
#   project_roles = [
#     "${google_project.conda_cps_prod.project_id}=>roles/bigquery.dataEditor",
#     "${google_project.conda_cps_prod.project_id}=>roles/bigquery.jobUser",
#   ]
# }
