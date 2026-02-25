#!/bin/bash

cd gcp-resources/terraform

echo "=== Terraform init ==="
terraform init

echo "=== Terraform plan ==="
terraform plan

BRANCH_NAME=$(git branch --show-current)
if [ "${BRANCH_NAME}" == "main" ]; then
  terraform apply -auto-approve
fi
