#!/bin/bash

cd gcp-resources/terraform

echo "=== Terraform init ==="
terraform init

echo "=== Terraform plan ==="
terraform plan

if [ "${BRANCH_NAME}" == "main" ]; then
  terraform apply -auto-approve
fi
