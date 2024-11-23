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
    df_ref = spark.read.parquet("hdfs://namenode:9000/staging/reference/" + str(year) + "/" + str(month) + "/" + str(day))
    df_tmp_ref = df_ref.withColumn("hash_key_origin", sha2("Origin", 256)).withColumn("hash_key_origin_state", sha2("OriginState", 256)).withColumn("hash_key_dest", sha2("Dest", 256)).withColumn("hash_key_dest_state", sha2("DestState", 256))
    df_tmp_ref.createOrReplaceTempView("view_tmp_ref")

    spark.sql("insert into table hub_origin select distinct hash_key_origin as hash_key, FlightDate as load_date, 'reference data' as record_source from view_tmp_ref")
    spark.sql("insert into table hub_origin_state select distinct hash_key_origin_state as hash_key, FlightDate as load_date, 'reference data' as record_source from view_tmp_ref")
    spark.sql("insert into table hub_dest select distinct hash_key_dest as hash_key, FlightDate as load_date, 'reference data' as record_source from view_tmp_ref")
    spark.sql("insert into table hub_dest_state select distinct hash_key_dest_state as hash_key, FlightDate as load_date, 'reference data' as record_source from view_tmp_ref")

    spark.sql("""insert into table sat_origin
              select distinct hash_key_origin as hash_key, FlightDate as load_date, 'reference data' as record_source,
              Origin as origin, OriginCityName as origin_city_name, FlightDate as flight_date
              from view_tmp_ref""")
    spark.sql("""insert into table sat_origin_state
              select distinct hash_key_origin_state as hash_key, FlightDate as load_date, 'reference data' as record_source,
              OriginState as origin_state, OriginStateName as origin_state_name, FlightDate as flight_date
              from view_tmp_ref""")
    spark.sql("""insert into table sat_dest
              select distinct hash_key_dest as hash_key, FlightDate as load_date, 'reference data' as record_source,
              Dest as dest, DestCityName as dest_city_name, FlightDate as flight_date
              from view_tmp_ref""")
    spark.sql("""insert into table sat_dest_state
              select distinct hash_key_dest_state as hash_key, FlightDate as load_date, 'reference data' as record_source,
              DestState as dest_state, DestStateName as dest_state_name, FlightDate as flight_date
              from view_tmp_ref""")

    increase_time()

if __name__ == "__main__":
    datawarehouse_location = 'hdfs://namenode:9000/datawarehouse'
    spark = SparkSession.builder.appName("Insert reeference data").config("spark.sql.warehouse.dir", datawarehouse_location).enableHiveSupport().getOrCreate()
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