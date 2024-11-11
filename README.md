# Data Lake with Hadoop Ecosystem

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

## Data flow
  <img src="https://github.com/Tran-Ngoc-Bao/Hadoop_Ecosystem/blob/master/pictures/system.png">

## Deploy
### 1. Install Kubernetes
```
https://phoenixnap.com/kb/kubernetes-on-windows
```

### 2. Install Helm
```
https://phoenixnap.com/kb/install-helm
```

### 3. Install WSL
```
https://kubernetes.io/blog/2020/05/21/wsl-docker-kubernetes-on-the-windows-desktop/
```

### 4. Create a Kubernetes Cluster with Minikube
#### 4.1. Create a Cluster
```sh
minikube start --cpus 4 --memory 8192 --nodes 3 -p hadoop-ecosystem
```

#### 4.2. Label Nodes
```sh
kubectl label node hadoop-ecosystem-m02 node-role.kubernetes.io/worker=worker; kubectl label nodes hadoop-ecosystem-m02 role=worker
```
```sh
kubectl label node hadoop-ecosystem-m03 node-role.kubernetes.io/worker=worker; kubectl label nodes hadoop-ecosystem-m03 role=worker
```

### 5. Deploy system
#### 5.1. Deploy Airflow
```sh
helm install airflow ./kubernetes/helm/airflow -n hadoop-ecosystem --create-namespace
```

#### 5.2. Deploy Hadoop
```sh
helm install hadoop ./kubernetes/helm/hadoop -n hadoop-ecosystem
```

#### 5.3. Deploy Hive
```sh
helm install hive-metastore ./kubernetes/helm/hive-metastore -n hadoop-ecosystem
```

#### 5.4. Deploy Trino
```sh
helm install trino ./kubernetes/helm/trino -n hadoop-ecosystem
```

#### 5.5. Deploy Superset
```sh
helm install superset ./kubernetes/helm/superset -n hadoop-ecosystem
```

## Demo

## Report
