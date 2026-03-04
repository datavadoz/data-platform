locals {
  cps_image = "${var.docker_registry}/${var.cps_image_name}:${var.cps_image_version}"

  cps_activate_apis = [
    "bigquery.googleapis.com",
    "cloudbuild.googleapis.com",
    "iam.googleapis.com",
    "run.googleapis.com",
  ]

  cps_cloudrun_roles = [
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/iam.serviceAccountTokenCreator",
  ]
}
