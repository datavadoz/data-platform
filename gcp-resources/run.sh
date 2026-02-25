#!/bin/bash

cd gcp-resources/terraform

echo "=== Terraform init ==="
terraform init

echo "=== Terraform plan ==="
terraform plan

echo "=== Terraform apply ==="
if [ "${BRANCH_NAME}" == "\${main}" ]; then
  terraform apply -auto-approve
else
  echo "Ignore TF apply for ${BRANCH_NAME}"
fi
