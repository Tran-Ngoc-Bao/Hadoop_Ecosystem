from pyspark.sql import SparkSession
from sys import argv

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Push file").getOrCreate()

    year = int(argv[1])
    month = int(argv[2])

    if (year == 2022 and month >= 8) or year > 2022:
        pass
    else:
        df = spark.read.option("header", "true").csv("/opt/airflow/source/flight_data/raw/Flights_" + str(year) + "_" + str(month) + ".csv")
        df.repartition(1).write.mode("overwrite").parquet("hdfs://namenode:9000/staging/" + str(year) + "/" + str(month))
        