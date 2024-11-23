from pyspark.sql import SparkSession

if __name__ == "__main__":
    datawarehouse_location = 'hdfs://namenode:9000/datawarehouse'
    spark = SparkSession.builder.appName("Truncate data warehouse").config("spark.sql.warehouse.dir", datawarehouse_location).enableHiveSupport().getOrCreate()
    
    # Reference data
    spark.sql("use data_warehouse")

    spark.sql("truncate table hub_origin")
    
    spark.sql("truncate table hub_origin_state")
    
    spark.sql("truncate table hub_dest")
    
    spark.sql("truncate table hub_dest_state")
    
    spark.sql("truncate table sat_origin")
    
    spark.sql("truncate table sat_origin_state")
    
    spark.sql("truncate table sat_dest")
    
    spark.sql("truncate table sat_dest_state")
    
    # Transaction data
    spark.sql("truncate table hub_flight")

    spark.sql("truncate table sat_marketing")
    
    spark.sql("truncate table sat_operating")
    
    spark.sql("truncate table sat_origin_tran")
    
    spark.sql("truncate table sat_dest_tran")
    
    spark.sql("truncate table sat_departure")
    
    spark.sql("truncate table sat_taxi_wheels")
    
    spark.sql("truncate table sat_arrival")
    
    spark.sql("truncate table sat_cancelled")
    
    spark.sql("truncate table sat_distance")
    
    spark.sql("truncate table sat_reason_delay")
    
    spark.sql("truncate table sat_other_time")

    spark.sql("truncate table pit_flight")

    # Link
    spark.sql("truncate table lnk_flight_origin")

    spark.sql("truncate table lnk_flight_origin_state")

    spark.sql("truncate table lnk_flight_dest")

    spark.sql("truncate table lnk_flight_dest_state")