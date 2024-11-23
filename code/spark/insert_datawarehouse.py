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
    # Reference data solution
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
    
    # Transaction data solution
    df_tran = spark.read.parquet("hdfs://namenode:9000/staging/transaction/" + str(year) + "/" + str(month) + "/" + str(day))
    df_tmp_tran = df_tran.withColumn("hash_key", sha2("id", 256))
    df_tmp_tran.createOrReplaceTempView("view_tmp_tran")

    spark.sql("insert into table hub_flight select hash_key, FlightDate as load_date, 'transaction data' as record_source from view_tmp_tran")

    spark.sql("""insert into table sat_marketing
              select hash_key, FlightDate as load_date, 'transaction data' as record_source,
              Marketing_Airline_Network as marketing_airline_network, Operated_or_Branded_Code_Share_Partners as operated_or_branded_code_share_partners, DOT_ID_Marketing_Airline as dot_id_marketing_airline,
              IATA_Code_Marketing_Airline as iata_code_marketing_airline, Flight_Number_Marketing_Airline as flight_number_marketing_airline
              from view_tmp_tran""")
    
    spark.sql("""insert into table sat_operating
              select hash_key, FlightDate as load_date, 'transaction data' as record_source,
              "Operating_Airline " as operating_airline, DOT_ID_operating_Airline as dot_id_operating_airline,
              IATA_Code_operating_Airline as iata_code_operating_airline, Tail_Number as tail_number, Flight_Number_operating_Airline as flight_number_operating_airline
              from view_tmp_tran""")
    
    spark.sql("""insert into table sat_origin_tran
              select hash_key, FlightDate as load_date, 'transaction data' as record_source,
              OriginAirportID as origin_airport_id, OriginAirportSeqID as origin_airport_seq_id, OriginCityMarketID as origin_city_market_id,
              Origin as origin, OriginState as origin_state, OriginStateFips as origin_state_fips, OriginWac as origin_wac
              from view_tmp_tran""")
    
    spark.sql("""insert into table sat_dest_tran
              select hash_key, FlightDate as load_date, 'transaction data' as record_source,
              DestAirportID as dest_airport_id, DestAirportSeqID as dest_airport_seq_id, DestCityMarketID as dest_city_market_id,
              Dest as Dest, DestState as dest_state, DestStateFips as dest_state_fips, DestWac as dest_wac
              from view_tmp_tran""")
    
    spark.sql("""insert into table sat_departure
              select hash_key, FlightDate as load_date, 'transaction data' as record_source,
              CRSDepTime as crs_dep_time, DepTime as dep_time, DepDelay as dep_delay, DepDelayMinutes as dep_delay_minutes, DepDel15 as dep_del_15, DepartureDelayGroups as departure_delay_groups, DepTimeBlk as dep_time_blk
              from view_tmp_tran""")
    
    spark.sql("""insert into table sat_taxi_wheels
              select hash_key, FlightDate as load_date, 'transaction data' as record_source,
              TaxiOut as taxi_out, WheelsOff as wheels_off, WheelsOn as wheels_on, TaxiIn as taxi_in
              from view_tmp_tran""")
    
    spark.sql("""insert into table sat_arrival
              select hash_key, FlightDate as load_date, 'transaction data' as record_source,
              CRSArrTime as crs_arr_time, ArrTime as arr_time, ArrDelay as arr_delay, ArrDelayMinutes as arr_delay_minutes, ArrDel15 as arr_del_15, ArrivalDelayGroups as arrival_delay_groups, ArrTimeBlk as arr_time_blk
              from view_tmp_tran""")
    
    spark.sql("""insert into table sat_cancelled
              select hash_key, FlightDate as load_date, 'transaction data' as record_source,
              Cancelled as cancelled, CancellationCode as cancellation_code
              from view_tmp_tran""")
    
    spark.sql("""insert into table sat_distance
              select hash_key, FlightDate as load_date, 'transaction data' as record_source,
              Distance as distance, DistanceGroup as distance_group
              from view_tmp_tran""")
    
    spark.sql("""insert into table sat_reason_delay
              select hash_key, FlightDate as load_date, 'transaction data' as record_source,
              CarrierDelay as carrier_delay, WeatherDelay as weather_delay, NASDelay as nas_delay, SecurityDelay as security_delay, LateAircraftDelay as late_aircraft_delay
              from view_tmp_tran""")
    
    spark.sql("""insert into table sat_other_time
              select hash_key, FlightDate as load_date, 'transaction data' as record_source,
              FirstDepTime as first_dep_time, TotalAddGTime as total_add_g_time, LongestAddGTime as longest_add_g_time,
              CRSElapsedTime as crs_elapsed_time, ActualElapsedTime as actual_elapsed_time, AirTime as air_time
              from view_tmp_tran""")

    spark.sql("""insert into table pit_flight
              select hash_key, FlightDate as load_date, 'transaction data' as record_source,
              Year as year, Quarter as quarter, DayofMonth as day_of_month, DayOfWeek as day_of_week, FlightDate as flight_date
              from view_tmp_tran""")
    
    # Link solution
    df_tmp_tran_flight = df_tmp_tran.select(col("hash_key").alias("hash_key_flight"), "Origin", "OriginState", "Dest", "DestState", "FlightDate")
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
    spark = SparkSession.builder.appName("Insert data warehouse").config("spark.sql.warehouse.dir", datawarehouse_location).enableHiveSupport().getOrCreate()
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