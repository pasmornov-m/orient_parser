FROM bitnami/spark:3.5.0

USER root
RUN install_packages curl

USER 1001
RUN curl -L https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar \
     -o /opt/bitnami/spark/jars/postgresql-42.7.5.jar
     # curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.12.262/aws-java-sdk-s3-1.12.262.jar \
     # -o /opt/bitnami/spark/jars/aws-java-sdk-s3-1.12.262.jar
     # curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
     # -o /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar && \
#     curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
#      -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar

USER 1001

# Редактируем конфигурацию Spark (например, добавляем JAR файлы в конфиг)
# RUN echo "spark.jars /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar,/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/bitnami/spark/jars/aws-java-sdk-s3-1.12.262.jar" >> /opt/bitnami/spark/conf/spark-defaults.conf
