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

resource "google_project" "cellphones_dev" {
  name            = "cellphones-dev"
  project_id      = "cellphones-dev"
  folder_id       = google_folder.cellphones.name
  deletion_policy = "DELETE"
}

resource "google_project" "cellphones_prod" {
  name            = "cellphones-prod"
  project_id      = "cellphones-prod"
  folder_id       = google_folder.cellphones.name
  deletion_policy = "DELETE"
}
