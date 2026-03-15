FROM python:3.13.12

RUN apt-get update && apt-get install -y git

COPY requirements.txt requirements.txt
COPY run.sh run.sh
COPY crawl.sh crawl.sh

RUN pip install -r requirements.txt
