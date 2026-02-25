#!/bin/bash

cd gcp-resources/terraform

terraform init
terraform plan

if [ "${BRANCH_NAME}" == "main" ]; then
  terraform apply -auto-approve
fi
