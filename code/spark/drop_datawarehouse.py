from pyspark.sql import SparkSession

if __name__ == "__main__":
    datawarehouse_location = 'hdfs://namenode:9000/datawarehouse'
    spark = SparkSession.builder.appName("Drop data warehouse").config("spark.sql.warehouse.dir", datawarehouse_location).enableHiveSupport().getOrCreate()
    
    # Reference data
    spark.sql("use data_warehouse")

    spark.sql("drop table if exists hub_origin")
    
    spark.sql("drop table if exists hub_origin_state")
    
    spark.sql("drop table if exists hub_dest")
    
    spark.sql("drop table if exists hub_dest_state")
    
    spark.sql("drop table if exists sat_origin")
    
    spark.sql("drop table if exists sat_origin_state")
    
    spark.sql("drop table if exists sat_dest")
    
    spark.sql("drop table if exists sat_dest_state")
    
    # Transaction data
    spark.sql("drop table if exists hub_flight")

    spark.sql("drop table if exists sat_marketing")
    
    spark.sql("drop table if exists sat_operating")
    
    spark.sql("drop table if exists sat_origin_tran")
    
    spark.sql("drop table if exists sat_dest_tran")
    
    spark.sql("drop table if exists sat_departure")
    
    spark.sql("drop table if exists sat_taxi_wheels")
    
    spark.sql("drop table if exists sat_arrival")
    
    spark.sql("drop table if exists sat_cancelled")
    
    spark.sql("drop table if exists sat_distance")
    
    spark.sql("drop table if exists sat_reason_delay")
    
    spark.sql("drop table if exists sat_other_time")

    spark.sql("drop table if exists pit_flight")

    # Link
    spark.sql("drop table if exists lnk_flight_origin")

    spark.sql("drop table if exists lnk_flight_origin_state")

    spark.sql("drop table if exists lnk_flight_dest")

    spark.sql("drop table if exists lnk_flight_dest_state")

    spark.sql("drop schema if exists data_warehouse")