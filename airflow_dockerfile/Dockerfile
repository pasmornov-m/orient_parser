FROM apache/airflow:2.10.3

COPY requirements.txt /requirements.txt

RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir \
    --retries 5 \
    --timeout 100 \
    --resume-retries 5 \
    -r /requirements.txt

USER root
RUN apt update && \
    apt install -y openjdk-17-jdk curl

USER airflow