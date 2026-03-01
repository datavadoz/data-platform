resource "google_folder" "cellphones" {
  display_name = "CellphoneS"
  parent       = "organizations/138569242965"
}

### DEV ###
module "prj_cps_dev" {
  source  = "terraform-google-modules/project-factory/google"
  version = "18.2"

  name            = "cps-dev"
  billing_account = "01DDAB-3D0A6F-F91FAF"
  deletion_policy = "DELETE"
  folder_id       = google_folder.cellphones.name
}

module "sa_cps_cloudrun_dev" {
  source        = "terraform-google-modules/service-accounts/google"
  version       = "4.7"
  project_id    = module.prj_cps_dev.project_id
  prefix        = "sa"
  names         = ["cloudrun"]
  project_roles = [
    "${module.prj_cps_dev.project_id}=>roles/bigquery.dataEditor",
    "${module.prj_cps_dev.project_id}=>roles/bigquery.jobUser",
  ]
}

### PROD ###
module "prj_cps_prod" {
  source  = "terraform-google-modules/project-factory/google"
  version = "18.2"

  name            = "cps-prod"
  billing_account = "01DDAB-3D0A6F-F91FAF"
  deletion_policy = "DELETE"
  folder_id       = google_folder.cellphones.name
}

module "sa_cps_cloudrun_prod" {
  source        = "terraform-google-modules/service-accounts/google"
  version       = "4.7"
  project_id    = module.prj_cps_prod.project_id
  prefix        = "sa"
  names         = ["cloudrun"]
  project_roles = [
    "${module.prj_cps_prod.project_id}=>roles/bigquery.dataEditor",
    "${module.prj_cps_prod.project_id}=>roles/bigquery.jobUser",
  ]
}
