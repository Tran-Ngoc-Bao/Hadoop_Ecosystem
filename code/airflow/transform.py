from airflow import DAG
from airflow.operators.bash_operator import BashOperator # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
from datetime import timedelta
from pyspark.sql import SparkSession

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": "2024-11-28 13:30:00",
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG("transform", default_args=default_args, schedule_interval="*/2 * * * *", max_active_runs=1)

year = 2030
month = 1

spark = SparkSession.builder.appName("Get time from HDFS").getOrCreate()
df = spark.read.option("header", "true").csv("hdfs://namenode:9000/time")
time = df.first()
if time is not None:
    year = int(time["year"])
    month = int(time["month"])

def increase_time_def():
    global year
    global month

    if year == 2030:
        pass
    else:
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
        
        columns = ["year", "month"]
        data = [(year, month)]
        df = spark.createDataFrame(data, columns)
        df.repartition(1).write.option("header", "true").mode("overwrite").csv("hdfs://namenode:9000/time")

transform_data = BashOperator(
    task_id="transform_data",
    bash_command=f"source /opt/airflow/source/env.sh && spark-submit /opt/airflow/spark/transform_data_airflow.py {year} {month}",
    dag=dag
)

increase_time = PythonOperator(
    task_id="increase_time",
    python_callable=increase_time_def,
    dag=dag
)

transform_data >> increase_time
