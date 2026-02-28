FROM ubuntu:26.04

# Install required softwares
RUN apt update -y && \
    apt install -y wget unzip python3 curl git

# Install Terraform
RUN mkdir /tmp/terraform && \
    cd /tmp/terraform && \
    wget https://releases.hashicorp.com/terraform/1.14.5/terraform_1.14.5_linux_amd64.zip && \
    unzip terraform_1.14.5_linux_amd64.zip && \
    mv /tmp/terraform/terraform /usr/local/bin && \
    cd - && \
    rm -r /tmp/terraform/

# Install Google Cloud CLI
RUN mkdir /root/gcloud && \
    cd /root/gcloud && \
    curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz && \
    tar -xf google-cloud-cli-linux-x86_64.tar.gz && \
    ./google-cloud-sdk/install.sh  --quiet --path-update true --command-completion true  && \
    cd - && \
    rm /root/gcloud/google-cloud-cli-linux-x86_64.tar.gz


