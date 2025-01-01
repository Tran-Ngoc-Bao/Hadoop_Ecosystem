FROM bitnami/airflow:2.10.3-debian-12-r0
USER root
RUN apt update && apt install -y default-jdk
COPY env.sh /opt/env.sh
COPY trino /opt/trino
RUN chmod +x /opt/trino
RUN mkdir /tmp/.ivy
RUN pip install pyspark confluent_kafka