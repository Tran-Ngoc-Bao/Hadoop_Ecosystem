### 1. Download Spark & Hadoop packages and Replace config in builded-airflow/spark & builded-airflow/hadoop
```
https://spark.apache.org/downloads.html
```
```
https://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz
```

### 2. Move Spark and Hadoop packages to Airflow
```sh
kubectl cp /path/to/spark <pod-airflow-webserver>:/tmp & kubectl cp /path/to/hadoop <pod-airflow-webserver>:/tmp
```
```
Similar for pod airflow-scheduler and airflow-worker
```

### 3. Locate Spark and Hadoop packages in Airflow
```
Exec to root of container airflow-webserver of pod airflow webserver
```
```sh
mv /tmp/spark /opt && mv /tmp/hadoop /opt
```
```
Similar for pod airflow scheduler and airflow worker
```

### 4. Move source code to Airflow
```sh
kubectl cp /path/to/sourcecode <pod-airflow-webserver>:/tmp
```
```
Similar for pod airflow scheduler and airflow worker
```

### 5. Locate source code in Airflow
```
Exec to root of container airflow-webserver of pod airflow webserver
```
```sh
mv /tmp/code /opt/bitnami/airflow
```
```sh
cp /tmp/code/airflow/*.py /opt/bitnami/airflow/dags
```
```
Similar for pod airflow scheduler and airflow worker
```
