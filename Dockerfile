FROM bitnami/airflow:2.10.3-debian-12-r0
USER root
RUN apt update && apt install -y default-jdk
COPY hadoop /opt/hadoop
COPY spark /opt/spark
RUN touch /opt/env.sh
RUN echo "PATH=/opt/spark/bin:$PATH" >> /opt/env.sh
RUN echo "export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop" >> /opt/env.sh && echo "export SPARK_HOME=/opt/spark" >> /opt/env.sh
RUN echo "export LD_LIBRARY_PATH=/opt/hadoop/lib/native:$LD_LIBRARY_PATH" >> /opt/env.sh