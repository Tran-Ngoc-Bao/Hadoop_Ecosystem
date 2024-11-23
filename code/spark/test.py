from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Test").getOrCreate()
    df = spark.read.parquet("hdfs://namenode:9000/staging/transaction/2018/1/1").printSchema()
