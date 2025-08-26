FROM apache/airflow:2.10.3

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk \
        curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt .

RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir \
    --retries 5 \
    --timeout 100 \
    --resume-retries 5 \
    -r requirements.txt
