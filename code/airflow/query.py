from airflow import DAG
from airflow.operators.bash_operator import BashOperator # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
from datetime import timedelta
from pyspark.sql import SparkSession
import os

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

def query_data_def():
    global year
    global month

    if (year == 2022 and month >= 8) or year > 2022:
        pass
    else:
        string = ''
        with open('/opt/airflow/sql/trino/template/month.sql', 'r') as f:
            string = f.read()

        string_replace = string.replace('{year}', str(year)).replace('{month}', str(month))
        with open(f'/opt/airflow/sql/trino/month/month_{year}_{month}.sql', 'w') as f:
            f.write(string_replace)

        os.system(f'cd /opt/airflow/source && ./trino --server http://trino:8080 --file /opt/airflow/sql/trino/month/month_{year}_{month}.sql')

        if month % 3 == 0:
            string = ''
            with open('/opt/airflow/sql/trino/template/quarter.sql', 'r') as f:
                string = f.read()

            quarter = int(month / 3)
            string_replace = string.replace('{year}', str(year)).replace('{quarter}', str(quarter))
            with open(f'/opt/airflow/sql/trino/quarter/quarter_{year}_{quarter}.sql', 'w') as f:
                f.write(string_replace)
            
            os.system(f'cd /opt/airflow/source && ./trino --server http://trino:8080 --file /opt/airflow/sql/trino/quarter/quarter_{year}_{quarter}.sql')

            if month % 12 == 0:
                string = ''
                with open('/opt/airflow/sql/trino/template/year.sql', 'r') as f:
                    string = f.read()

                string_replace = string.replace('{year}', str(year))
                with open(f'/opt/airflow/sql/trino/year/year_{year}.sql', 'w') as f:
                    f.write(string_replace)
                
                os.system(f'cd /opt/airflow/source && ./trino --server http://trino:8080 --file /opt/airflow/sql/trino/year/year_{year}.sql')

query_data = PythonOperator(
    task_id="query_data",
    python_callable=query_data_def,
    dag=dag
)

increase_time = PythonOperator(
    task_id="increase_time",
    python_callable=increase_time_def,
    dag=dag
)

query_data >> increase_time