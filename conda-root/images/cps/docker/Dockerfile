FROM ubuntu:26.04

COPY requirements.txt requirements.txt

RUN apt update -y && \
    apt install -y --allow-remove-essential wget coreutils-from-gnu coreutils-from-uutils- && \
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash Miniconda3-latest-Linux-x86_64.sh -b -c && \
    rm Miniconda3-latest-Linux-x86_64.sh

RUN source ~/.bashrc && \
    conda tos accept && \
    conda create --name cps python=3.13 -y && \
    conda activate cps && \
    pip install --no-cache-dir -r requirements.txt
