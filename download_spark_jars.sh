#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_DIR="${SCRIPT_DIR}/spark_jars"

mkdir -p "$JAR_DIR"

curl -L -o "$JAR_DIR/hadoop-aws-3.3.4.jar" \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

curl -L -o "$JAR_DIR/aws-java-sdk-bundle-1.12.262.jar" \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

curl -L -o "$JAR_DIR/postgresql-42.7.5.jar" \
  https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar

curl -L -o "$JAR_DIR/wildfly-openssl-1.0.7.Final.jar" \
  https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar

curl -L -o "$JAR_DIR/checker-qual-3.48.3.jar" \
  https://repo1.maven.org/maven2/org/checkerframework/checker-qual/3.48.3/checker-qual-3.48.3.jar
