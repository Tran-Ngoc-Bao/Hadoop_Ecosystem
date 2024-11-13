from airflow import DAG
from airflow.operators.bash_operator import BashOperator # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
from datetime import datetime, timedelta
from pyspark.sql import SparkSession # type: ignore

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 10, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes = 1),
}

# dag = DAG("main", default_args = default_args, schedule_interval = "0 3,4,7,8 * * *")
dag = DAG("main", default_args = default_args, schedule_interval = timedelta(30))

def check_leap_year(y):
    yi = int(y)
    if yi % 4:
        return False
    if yi % 400 == 0:
        return True
    if yi % 100 == 0:
        return False
    return True

def increase_time(y, m, d):
    yi = int(y)
    mi = int(m)
    di = int(d)
    if m == "12" and d == "31":
        return str(yi + 1) + " 1 1"
    if d == "31":
        return y + " " + str(mi + 1) + " 1"
    if m == "2" and (d == "29" or (check_leap_year(yi) == False and d == "28")):
        return y + " 3 1"
    if d == "30" and mi in [4, 6, 9, 11]:
        return y + " " + str(mi + 1) + " 1"
    return y + " " + m + " " + str(di + 1)

def solution():
    f = open("/opt/airflow/source/time.txt", "r+")
    s = f.read().split(" ")
    year = s[0]
    month = s[1]
    day = s[2]

    spark = SparkSession.builder.appName("Hello").getOrCreate()
    df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("/opt/airflow/source/flight_data/raw/Flights_" + year + "_" + month + ".csv")
    result = df.filter("DayofMonth = {}".format(day))
    result.repartition(1).write.mode("append").parquet("hdfs://namenode:9000/" + year + "/" + month)

    f.seek(0)
    f.write(increase_time(year, month, day))
    f.close()

crawl_data = PythonOperator(
    task_id = "crawl_data",
    python_callable = solution, 
    dag = dag
)

crawl_data