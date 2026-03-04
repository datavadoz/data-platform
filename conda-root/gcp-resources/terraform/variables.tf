variable "org_id" {
  description = "The organization ID"
  type        = string
  default     = "138569242965"
}

variable "billing_account" {
  description = "The billing account ID"
  type        = string
  default     = "01DDAB-3D0A6F-F91FAF"
}

variable "region" {
  description = "The default GCP region"
  type        = string
  default     = "asia-southeast1"
}

variable "owner_email" {
  description = "The owner email for IAM bindings"
  type        = string
  default     = "danh.vo@conda.fun"
}

variable "docker_registry" {
  description = "The Docker registry URL"
  type        = string
  default     = "asia-southeast1-docker.pkg.dev/conda-root/data-platform"
}

variable "cps_image_name" {
  description = "The CPS Docker image name"
  type        = string
  default     = "cps"
}

variable "cps_image_version" {
  description = "The CPS Docker image version"
  type        = string
  default     = "0.0.1"
}
