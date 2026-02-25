#!/bin/bash

cd gcp-resources/terraform

echo "=== Terraform init ==="
terraform init

echo "=== Terraform plan ==="
terraform plan

echo "=== Terraform apply ==="
BRANCH_NAME=$(git branch --show-current)
if [ "${BRANCH_NAME}" == "main" ]; then
  terraform apply -auto-approve
else
  echo "Ignore TF apply for ${BRANCH_NAME}"
fi
