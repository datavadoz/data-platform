locals {
  cps_image = "${var.docker_registry}/${var.cps_image_name}:${var.cps_image_version}"
}
