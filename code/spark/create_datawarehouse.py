from pyspark.sql import SparkSession

if __name__ == "__main__":
    datawarehouse_location = 'hdfs://namenode:9000/datawarehouse'
    spark = SparkSession.builder.appName("Create data warehou   se").config("spark.sql.warehouse.dir", datawarehouse_location).enableHiveSupport().getOrCreate()
    
    # Reference data
    spark.sql("create schema if not exists data_warehouse")
    spark.sql("use data_warehouse")

    spark.sql("""create table if not exists hub_origin
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255))""")
    
    spark.sql("""create table if not exists hub_origin_state
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255))""")
    
    spark.sql("""create table if not exists hub_dest
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255))""")
    
    spark.sql("""create table if not exists hub_dest_state
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255))""")
    
    spark.sql("""create table if not exists sat_origin
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255),
              origin varchar(255), origin_city_name varchar(255), flight_date varchar(255))""")
    
    spark.sql("""create table if not exists sat_origin_state
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255),
              origin_state varchar(255), origin_state_name varchar(255), flight_date varchar(255))""")
    
    spark.sql("""create table if not exists sat_dest
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255),
              dest varchar(255), dest_city_name varchar(255), flight_date varchar(255))""")
    
    spark.sql("""create table if not exists sat_dest_state
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255),
              dest varchar(255), dest_state_name varchar(255), flight_date varchar(255))""")
    
    # Transaction data
    spark.sql("""create table if not exists hub_flight
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255))""")

    spark.sql("""create table if not exists sat_marketing
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255),
              marketing_airline_network varchar(255), operated_or_branded_code_share_partners varchar(255), dot_id_marketing_airline double, iata_code_marketing_airline varchar(255), flight_number_marketing_airline double)""")
    
    spark.sql("""create table if not exists sat_operating
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255),
              operating_airline varchar(255), dot_id_operating_airline double, iata_code_operating_airline varchar(255), tail_number varchar(255), flight_number_operating_airline double)""")
    
    spark.sql("""create table if not exists sat_origin_tran
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255),
              origin_airport_id double, origin_airport_seq_id double, origin_city_market_id double,
              origin varchar(255), origin_state varchar(255), origin_state_fips double, origin_wac double)""")
    
    spark.sql("""create table if not exists sat_dest_tran
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255),
              dest_airport_id double, dest_airport_seq_id double, dest_city_market_id double,
              dest varchar(255), dest_state varchar(255), dest_state_fips double, dest_wac double)""")
    
    spark.sql("""create table if not exists sat_departure
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255),
              crs_dep_time double, dep_time double, dep_delay double, dep_delay_minutes double, dep_del_15 double, departure_delay_groups double, dep_time_blk varchar(255))""")
    
    spark.sql("""create table if not exists sat_taxi_wheels
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255),
              taxi_out double, wheels_off double, wheels_on double, taxi_in double)""")
    
    spark.sql("""create table if not exists sat_arrival
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255),
              crs_arr_time double, arr_time double, arr_delay double, arr_delay_minutes double, arr_del_15 double, arrival_delay_groups double, arr_time_blk varchar(255))""")
    
    spark.sql("""create table if not exists sat_cancelled
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255),
              cancelled double, cancellation_code varchar(255))""")
    
    spark.sql("""create table if not exists sat_distance
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255),
              distance double, distance_group double)""")
    
    spark.sql("""create table if not exists sat_reason_delay
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255),
              carrier_delay double, weather_delay double, nas_delay double, security_delay double, late_aircraft_delay double)""")
    
    spark.sql("""create table if not exists sat_other_time
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255),
              first_dep_time double, total_add_g_time double, longest_add_g_time double,
              crs_elapsed_time double, actual_elapsed_time double, air_time double)""")

    spark.sql("""create table if not exists pit_flight
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255),
              year double, quarter double, day_of_month double, day_of_week double, flight_date varchar(255))""")
    
    # Link
    spark.sql("""create table if not exists lnk_flight_origin
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255),
              hash_key_flight varchar(255), hash_key_origin varchar(255))""")

    spark.sql("""create table if not exists lnk_flight_origin_state
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255),
              hash_key_flight varchar(255), hash_key_origin_state varchar(255))""")
    
    spark.sql("""create table if not exists lnk_flight_dest
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255),
              hash_key_flight varchar(255), hash_key_dest varchar(255))""")
    
    spark.sql("""create table if not exists lnk_flight_dest_state
              (hash_key varchar(255), load_date varchar(255), record_source varchar(255),
              hash_key_flight varchar(255), hash_key_dest_state varchar(255))""")