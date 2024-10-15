# Data Lake with Hadoop Ecosystem

## Introduction
<ul>
  <li>Name of project: Learn about Hadoop ecosystem components. Apply to real-world problems: Build a Data Lake system to analyze flight data on Kaggle</li>
  <li>Project objective:
    <ul>
      <li>Explore Hadoop ecosystem components in big data processing storage with Data Lake architecture</li>
      <li>Mastering Hadoop system administration for Flight data analysis</li>
      <li>Monitor performance, fault tolerance, load balancing, and information security issues</li>
    </ul>
  </li>
</ul>

## Data flow
  <img src="https://github.com/Tran-Ngoc-Bao/Hadoop_Ecosystem/blob/master/pictures/system.png">

## Deploy system
#### 1. You should pull and build images in file docker-compose.yaml before

#### 2. Move to clone project and Start system
  
```sh
docker compose up -d
```

#### 3. Build enviroment on airflow-webserve and airflow-scheduler

```sh
docker exec -u root -it [airflow-webserver/airflow-scheduler] bash 
source /opt/airflow/source/build-env.sh
```

#### 4. After start system, all port website of containers in <a href="https://github.com/Tran-Ngoc-Bao/Hadoop_Ecosystem/blob/master/port.txt">here</a>

#### 5. Start DAG in Airflow cluster

#### 6. Build enviroment Superset
```sh
./superset/bootstrap-superset.sh
```
  
#### 7. Visualize data in Superset with SQLalchemy uri
```sh
trino://hive@trino:8080/hive
```

## Demo

## Report
