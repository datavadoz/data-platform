#!/bin/bash

cd gcp-resources/terraform

echo "=== Terraform init ==="
terraform init -upgrade || exit 1

echo "=== Terraform plan ==="
terraform plan || exit 1

echo "=== Terraform apply ==="
if [ "${BRANCH_NAME}" == "\${main}" ]; then
  terraform apply -auto-approve
else
  echo "Ignore TF apply for ${BRANCH_NAME}"
fi
