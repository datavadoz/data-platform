FROM ubuntu:26.04

COPY requirements.txt requirements.txt

RUN apt update -y && \
    apt install -y --allow-remove-essential wget coreutils-from-gnu coreutils-from-uutils- && \
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash Miniconda3-latest-Linux-x86_64.sh -b -c && \
    rm Miniconda3-latest-Linux-x86_64.sh
