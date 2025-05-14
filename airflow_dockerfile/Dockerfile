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


# RUN mkdir -p /opt/airflow/jars

# RUN curl -L -o /opt/airflow/jars/hadoop-aws-3.3.4.jar \
#     https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
#     curl -L -o /opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar \
#     https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
#     curl -L -o /opt/airflow/jars/postgresql-42.7.5.jar \
#     https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar && \
#     curl -L -o /opt/airflow/jars/wildfly-openssl-1.0.7.Final.jar \
#     https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar && \
#     curl -L -o /opt/airflow/jars/checker-qual-3.48.3.jar \
#     https://repo1.maven.org/maven2/org/checkerframework/checker-qual/3.48.3/checker-qual-3.48.3.jar

# RUN chown -R airflow: /opt/airflow/jars

RUN mkdir -p /home/airflow/.ivy2/jars

RUN curl -L -o /home/airflow/.ivy2/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /home/airflow/.ivy2/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -L -o /home/airflow/.ivy2/jars/postgresql-42.7.5.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar && \
    curl -L -o /home/airflow/.ivy2/jars/wildfly-openssl-1.0.7.Final.jar \
    https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar && \
    curl -L -o /home/airflow/.ivy2/jars/checker-qual-3.48.3.jar \
    https://repo1.maven.org/maven2/org/checkerframework/checker-qual/3.48.3/checker-qual-3.48.3.jar

RUN chown -R airflow: /home/airflow/.ivy2

USER airflow