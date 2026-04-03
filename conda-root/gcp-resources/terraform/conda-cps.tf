resource "google_folder" "cellphones" {
  display_name = "CellphoneS"
  parent       = "organizations/${var.org_id}"
}

### DEV ###
module "prj_conda_cps_dev" {
  source  = "terraform-google-modules/project-factory/google"
  version = "18.2"

  name            = "conda-cps-dev"
  billing_account = var.billing_account
  deletion_policy = "DELETE"
  folder_id       = google_folder.cellphones.name
  activate_apis   = [
    "bigquery.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudscheduler.googleapis.com",
    "compute.googleapis.com",
    "iam.googleapis.com",
    "run.googleapis.com",
    "secretmanager.googleapis.com",
  ]
}

module "prj_conda_cps_iam_dev" {
  source  = "terraform-google-modules/iam/google//modules/projects_iam"
  version = "8.2"

  projects = [module.prj_conda_cps_dev.project_id]

  bindings = {
    "roles/owner" = [
      "user:${var.owner_email}"
    ]
  }
}

module "sa_conda_cps_cloudrun_dev" {
  source        = "terraform-google-modules/service-accounts/google"
  version       = "4.7"
  project_id    = module.prj_conda_cps_dev.project_id
  prefix        = "sa"
  names         = ["cloudrun"]
  project_roles = [
    "${module.prj_conda_cps_dev.project_id}=>roles/bigquery.dataEditor",
    "${module.prj_conda_cps_dev.project_id}=>roles/bigquery.user",
    "${module.prj_conda_cps_dev.project_id}=>roles/iam.serviceAccountTokenCreator",
    "${module.prj_conda_cps_dev.project_id}=>roles/run.invoker",
    "${module.prj_conda_cps_dev.project_id}=>roles/secretmanager.secretAccessor",
  ]
}

resource "google_cloud_run_v2_job" "monitor_run_rate_dev" {
  project  = module.prj_conda_cps_dev.project_id
  name     = "monitor-run-rate"
  location = var.region

  template {
    template {
      max_retries     = 0
      service_account = module.sa_conda_cps_cloudrun_dev.email
      timeout         = "300s"

      containers {
        image   = local.cps_image
        command = ["/bin/bash"]
        args    = ["./run.sh", "dev"]

        env {
          name = "LARK_SECRET"
          value_source {
            secret_key_ref {
              secret  = "LARK_SECRET"
              version = "latest"
            }
          }
        }
      }
    }
  }
}

resource "google_cloud_scheduler_job" "monitor_run_rate_dev" {
  project          = module.prj_conda_cps_dev.project_id
  name             = "monitor-run-rate"
  region           = var.region
  schedule         = "30 9 * * *"
  time_zone        = "Asia/Ho_Chi_Minh"
  attempt_deadline = "320s"

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${module.prj_conda_cps_dev.project_id}/jobs/${google_cloud_run_v2_job.monitor_run_rate_dev.name}:run"

    oauth_token {
      service_account_email = module.sa_conda_cps_cloudrun_dev.email
    }
  }
}

resource "google_cloud_run_v2_job" "crawl_facebook_dev" {
  project  = module.prj_conda_cps_dev.project_id
  name     = "crawl-facebook"
  location = var.region

  template {
    template {
      max_retries     = 0
      service_account = module.sa_conda_cps_cloudrun_dev.email
      timeout         = "300s"

      containers {
        image   = local.cps_image
        command = ["/bin/bash"]
        args    = ["./crawl.sh", "dev", "facebook"]

        env {
          name = "PROXY_KEYS"
          value_source {
            secret_key_ref {
              secret  = "PROXY_KEYS"
              version = "latest"
            }
          }
        }
      }
    }
  }
}

module "bq_conda_cps_dev" {
  source  = "terraform-google-modules/bigquery/google"
  version = "10.2.1"

  project_id   = module.prj_conda_cps_dev.project_id
  dataset_id   = "external_gsheet"
  location     = var.region

  access = []
}

module "bq_conda_cps_ads_dev" {
  source  = "terraform-google-modules/bigquery/google"
  version = "10.2.1"

  project_id   = module.prj_conda_cps_dev.project_id
  dataset_id   = "ads"
  location     = var.region

  access = []
}

### PROD ###
module "prj_conda_cps_prod" {
  source  = "terraform-google-modules/project-factory/google"
  version = "18.2"

  name            = "conda-cps-prod"
  billing_account = var.billing_account
  deletion_policy = "DELETE"
  folder_id       = google_folder.cellphones.name
  activate_apis   = [
    "bigquery.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudscheduler.googleapis.com",
    "compute.googleapis.com",
    "iam.googleapis.com",
    "run.googleapis.com",
    "secretmanager.googleapis.com",
  ]
}

module "prj_conda_cps_iam_prod" {
  source  = "terraform-google-modules/iam/google//modules/projects_iam"
  version = "8.2"

  projects = [module.prj_conda_cps_prod.project_id]

  bindings = {
    "roles/owner" = [
      "user:${var.owner_email}"
    ]
  }
}

module "sa_conda_cps_cloudrun_prod" {
  source        = "terraform-google-modules/service-accounts/google"
  version       = "4.7"
  project_id    = module.prj_conda_cps_prod.project_id
  prefix        = "sa"
  names         = ["cloudrun"]
  project_roles = [
    "${module.prj_conda_cps_prod.project_id}=>roles/bigquery.dataEditor",
    "${module.prj_conda_cps_prod.project_id}=>roles/bigquery.user",
    "${module.prj_conda_cps_prod.project_id}=>roles/iam.serviceAccountTokenCreator",
    "${module.prj_conda_cps_prod.project_id}=>roles/run.invoker",
    "${module.prj_conda_cps_prod.project_id}=>roles/secretmanager.secretAccessor",
  ]
}

resource "google_cloud_run_v2_job" "monitor_run_rate_prod" {
  project  = module.prj_conda_cps_prod.project_id
  name     = "monitor-run-rate"
  location = var.region

  template {
    template {
      max_retries     = 0
      service_account = module.sa_conda_cps_cloudrun_prod.email
      timeout         = "300s"

      containers {
        image   = local.cps_image
        command = ["/bin/bash"]
        args    = ["./run.sh", "prod"]

        env {
          name = "LARK_SECRET"
          value_source {
            secret_key_ref {
              secret  = "LARK_SECRET"
              version = "latest"
            }
          }
        }
      }
    }
  }
}

resource "google_cloud_scheduler_job" "monitor_run_rate_prod" {
  project          = module.prj_conda_cps_prod.project_id
  name             = "monitor-run-rate"
  region           = var.region
  schedule         = "30 9 * * *"
  time_zone        = "Asia/Ho_Chi_Minh"
  attempt_deadline = "320s"

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${module.prj_conda_cps_prod.project_id}/jobs/${google_cloud_run_v2_job.monitor_run_rate_prod.name}:run"

    oauth_token {
      service_account_email = module.sa_conda_cps_cloudrun_prod.email
    }
  }
}

resource "google_cloud_run_v2_job" "crawl_facebook_prod" {
  project  = module.prj_conda_cps_prod.project_id
  name     = "crawl-facebook"
  location = var.region

  template {
    template {
      max_retries     = 0
      service_account = module.sa_conda_cps_cloudrun_prod.email
      timeout         = "300s"

      containers {
        image   = local.cps_image
        command = ["/bin/bash"]
        args    = ["./crawl.sh", "prod", "facebook"]

        env {
          name = "PROXY_KEYS"
          value_source {
            secret_key_ref {
              secret  = "PROXY_KEYS"
              version = "latest"
            }
          }
        }
      }
    }
  }
}

resource "google_cloud_scheduler_job" "crawl_facebook_prod" {
  project          = module.prj_conda_cps_prod.project_id
  name             = "crawl-facebook"
  region           = var.region
  schedule         = "0 8 * * 1"
  time_zone        = "Asia/Ho_Chi_Minh"
  attempt_deadline = "320s"

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${module.prj_conda_cps_prod.project_id}/jobs/${google_cloud_run_v2_job.crawl_facebook_prod.name}:run"

    oauth_token {
      service_account_email = module.sa_conda_cps_cloudrun_prod.email
    }
  }
}

module "bq_conda_cps_prod" {
  source  = "terraform-google-modules/bigquery/google"
  version = "10.2.1"

  project_id   = module.prj_conda_cps_prod.project_id
  dataset_id   = "external_gsheet"
  location     = var.region

  access = []
}

module "bq_conda_cps_ads_prod" {
  source  = "terraform-google-modules/bigquery/google"
  version = "10.2.1"

  project_id   = module.prj_conda_cps_prod.project_id
  dataset_id   = "ads"
  location     = var.region

  access = []
}
