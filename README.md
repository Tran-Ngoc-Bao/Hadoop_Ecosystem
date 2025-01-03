# IT4997 - Graduation thesis - SOICT- HUST

## Introduction
<ul>
  <li>Name of project: Building a Data Lake system to analyze flight data using Hadoop ecosystem components</li>
  <li>Project objective:
    <ul>
      <li>Explore Hadoop ecosystem components in big data processing storage with Data Lake architecture</li>
      <li>Mastering Hadoop system administration for Flight data analysis</li>
      <li>Monitor performance, fault tolerance, load balancing, and information security issues</li>
    </ul>
  </li>
</ul>

## System architecture
  <img src="https://github.com/Tran-Ngoc-Bao/Hadoop_Ecosystem/blob/master/pictures/system/system.png">

## Deploy
### 1. Install tools
#### 1.1 Install Kubernetes
```
https://phoenixnap.com/kb/kubernetes-on-windows
```

#### 1.2 Install Helm
```
https://phoenixnap.com/kb/install-helm
```

#### 1.3 Install WSL
```
https://kubernetes.io/blog/2020/05/21/wsl-docker-kubernetes-on-the-windows-desktop
```

#### 1.4 Install Lens
```
https://spacelift.io/blog/lens-kubernetes
```

### 2. Create a Kubernetes Cluster with Minikube
#### 2.1 Create a Cluster
```sh
minikube start --nodes 3 -p hadoop-ecosystem
```

#### 2.2 Label Nodes
```sh
kubectl label node hadoop-ecosystem-m02 node-role.kubernetes.io/worker=worker & kubectl label nodes hadoop-ecosystem-m02 role=worker
```
```sh
kubectl label node hadoop-ecosystem-m03 node-role.kubernetes.io/worker=worker & kubectl label nodes hadoop-ecosystem-m03 role=worker
```

### 3. Deploy system
#### 3.0 Create Namespace
```sh
kubectl create namespace hadoop-ecosystem & kubectl config set-context --current --namespace=hadoop-ecosystem
```

#### 3.1 Deploy Flask
```sh
kubectl create -f ./kubernetes/flask
```

#### 3.2 Deploy Kafka
```sh
helm install kafka ./kubernetes/kafka
```

#### 3.3 Deploy Airflow
```sh
helm install airflow ./kubernetes/airflow
```

#### 3.4 Deploy Hadoop
```sh
helm install hadoop ./kubernetes/hadoop
```

#### 3.5 Deploy Hive
```sh
helm install hive-metastore ./kubernetes/hive-metastore
```

#### 3.6 Deploy Trino
```sh
helm install trino ./kubernetes/trino
```

#### 3.7 Deploy Superset
```sh
helm install superset ./kubernetes/superset
```

### 4. Pre-use
#### 4.1 Download Data source
```
https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022/data?select=readme.md
```

#### 4.2 Move Data source to System
```sh
kubectl cp /path/to/datasource <pod-flask-1>:/data & kubectl cp /path/to/datasource <pod-flask-2>:/data
```

#### 4.3 Config Spark and Hadoop on Airflow
```
Read in readme.md in folder builded-airflow
```

## Demo

## Report

