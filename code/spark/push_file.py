from pyspark.sql import SparkSession

def solution():
    global year
    global month

    df = spark.read.option("header", "true").csv("/opt/airflow/source/flight_data/raw/Flights_" + str(year) + "_" + str(month) + ".csv")
    df.write.mode("overwrite").parquet("hdfs://namenode:9000/staging/" + str(year) + "/" + str(month))

    if month == 12:
        year += 1
        month = 1
    else:
        month += 1

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Push file").getOrCreate()

    year = 2018
    month = 1

    flag = True
    while flag:
        try:
            solution()
        except:
            print("Don't worry about this error", year, month)
            flag = False
    # solution()
    