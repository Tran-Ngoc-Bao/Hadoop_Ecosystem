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

### 1. Installing Kubernetes (on Windows)

```
https://phoenixnap.com/kb/kubernetes-on-windows
```

### 2. Create a 3 Node Kubernetes Cluster with Minikube

#### 2.1. Create a 3 Node Cluster

```sh
minikube start --nodes 3 -p hadoop-ecosystem
```

#### 2.2. Label Nodes

```sh
kubectl label node hadoop-ecosystem-m02 node-role.kubernetes.io/worker=worker
```

```sh
kubectl label node hadoop-ecosystem-m03 node-role.kubernetes.io/worker=worker
```

```sh
kubectl label nodes hadoop-ecosystem-m02 role=worker
```

```sh
kubectl label nodes hadoop-ecosystem-m03 role=worker
```

### 3. Deploy system


## Demo

## Report
