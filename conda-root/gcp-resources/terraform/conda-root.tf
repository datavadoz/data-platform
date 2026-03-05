module "prj_conda_root_iam_dev" {
  source  = "terraform-google-modules/iam/google//modules/projects_iam"
  version = "8.2"

  projects = ["conda-root"]

  bindings = {
    "roles/artifactregistry.reader" = [
      "serviceAccount:service-${module.prj_conda_cps_dev.project_number}@serverless-robot-prod.iam.gserviceaccount.com",
    ]
  }
}
