from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def check_leap_year():
    global year
    if year % 4:
        return False
    if year % 400 == 0:
        return True
    if year % 100 == 0:
        return False
    return True

def increase_time():
    global year
    global month
    global day
    if month == 12 and day == 31:
        year += 1
        month = 1
        day = 1
    elif day == 31:
        month += 1
        day = 1
    elif month == 2 and (day == 29 or (check_leap_year() == False and day == 28)):
        month = 3
        day = 1
    elif day == 30 and month in [4, 6, 9, 11]:
        month += 1
        day = 1
    else:
        day += 1

def solution():
    df_tran = spark.read.parquet("hdfs://namenode:9000/staging/transaction/" + str(year) + "/" + str(month) + "/" + str(day))
    df_tmp_tran = df_tran.select("id", "Origin", "OriginState", "Dest", "DestState", "FlightDate")
    df_tmp_tran_flight = df_tmp_tran.withColumn("hash_key_flight", sha2("id", 256))
    df_lnk_tran_flight = df_tmp_tran_flight.withColumn("hash_key_origin", sha2("Origin", 256)).withColumn("hash_key_origin_state", sha2("OriginState", 256)).withColumn("hash_key_dest", sha2("Dest", 256)).withColumn("hash_key_dest_state", sha2("DestState", 256))
    df_lnk_tran_flight_final = df_lnk_tran_flight.withColumn("hash_key_flight_origin", sha2(concat("hash_key_flight", "hash_key_origin"), 256)).withColumn("hash_key_flight_origin_state", sha2(concat("hash_key_flight", "hash_key_origin_state"), 256)).withColumn("hash_key_flight_dest", sha2(concat("hash_key_flight", "hash_key_dest"), 256)).withColumn("hash_key_flight_dest_state", sha2(concat("hash_key_flight", "hash_key_dest_state"), 256))
    df_lnk_tran_flight_final.createOrReplaceTempView("view_lnk_tran_flight_final")

    spark.sql("""insert into table lnk_flight_origin
              select hash_key_flight_origin as hash_key, FlightDate as load_date, 'reference data, transaction data' as record_source,
              hash_key_flight, hash_key_origin
              from view_lnk_tran_flight_final""")
    
    spark.sql("""insert into table lnk_flight_origin_state
              select hash_key_flight_origin_state as hash_key, FlightDate as load_date, 'reference data, transaction data' as record_source,
              hash_key_flight, hash_key_origin_state
              from view_lnk_tran_flight_final""")

    spark.sql("""insert into table lnk_flight_dest
              select hash_key_flight_dest as hash_key, FlightDate as load_date, 'reference data, transaction data' as record_source,
              hash_key_flight, hash_key_dest
              from view_lnk_tran_flight_final""")

    spark.sql("""insert into table lnk_flight_dest_state
              select hash_key_flight_dest_state as hash_key, FlightDate as load_date, 'reference data, transaction data' as record_source,
              hash_key_flight, hash_key_dest_state
              from view_lnk_tran_flight_final""")

    increase_time()

if __name__ == "__main__":
    datawarehouse_location = 'hdfs://namenode:9000/datawarehouse'
    spark = SparkSession.builder.appName("Insert link").config("spark.sql.warehouse.dir", datawarehouse_location).enableHiveSupport().getOrCreate()
    spark.sql("use data_warehouse")
    
    year = 2018
    month = 1
    day = 1
    flag = True
    while flag:
        try:
            solution()
        except:
            print("Don't worry about this error")
            flag = False
    # solution()