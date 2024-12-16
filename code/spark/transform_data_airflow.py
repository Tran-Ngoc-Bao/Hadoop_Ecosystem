from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from sys import argv

if __name__ == "__main__":
    datawarehouse_location = 'hdfs://namenode:9000/processed_data'
    spark = SparkSession.builder.appName("Transform data").config("spark.sql.warehouse.dir", datawarehouse_location).enableHiveSupport().getOrCreate()

    year = int(argv[1])
    month = int(argv[2])

    if (year == 2022 and month >= 8) or year > 2022:
        pass
    else:
        df = spark.read.parquet("hdfs://namenode:9000/staging/" + str(year) + "/" + str(month))

        # Select columns not too much null
        df_select = df.select("Year", "Quarter", "Month", "DayofMonth", "DayOfWeek", "FlightDate",
                            "Marketing_Airline_Network", "Operated_or_Branded_Code_Share_Partners", "DOT_ID_Marketing_Airline", "IATA_Code_Marketing_Airline", "Flight_Number_Marketing_Airline",
                            col("Operating_Airline ").alias("Operating_Airline"), "DOT_ID_Operating_Airline", "IATA_Code_Operating_Airline", "Tail_Number", "Flight_Number_Operating_Airline",
                            "OriginAirportID", "OriginAirportSeqID", "OriginCityMarketID", "Origin", "OriginCityName", "OriginState", "OriginStateFips", "OriginStateName", "OriginWac",                          "OriginAirportID", "OriginAirportSeqID", "OriginCityMarketID", "Origin", "OriginCityName", "OriginState", "OriginStateFips", "OriginStateName", "OriginWac",
                            "DestAirportID", "DestAirportSeqID", "DestCityMarketID", "Dest", "DestCityName", "DestState", "DestStateFips", "DestStateName", "DestWac",
                            "CRSDepTime", "DepTime", "DepDelay", "DepDelayMinutes", "DepDel15", "DepartureDelayGroups", "DepTimeBlk",
                            "TaxiOut", "WheelsOff", "WheelsOn", "TaxiIn",
                            "CRSArrTime", "ArrTime", "ArrDelay", "ArrDelayMinutes", "ArrDel15", "ArrivalDelayGroups", "ArrTimeBlk",
                            "Cancelled",
                            "CRSElapsedTime", "ActualElapsedTime", "AirTime",
                            "Distance", "DistanceGroup",
                            "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
                            "FirstDepTime", "TotalAddGTime", "LongestAddGTime"
                            )

        # Process duplicated rows
        df_duplicate = df_select.dropDuplicates()

        # Process date data
        df_process_date_tmp = df_duplicate \
            .withColumn("DepTimeBlkStart", when(length(df_duplicate.DepTimeBlk) == 9, substring(df_duplicate.DepTimeBlk, 1, 4).cast("int")).otherwise(-1)) \
            .withColumn("DepTimeBlkEnd", when(length(df_duplicate.DepTimeBlk) == 9, substring(df_duplicate.DepTimeBlk, 6, 4).cast("int")).otherwise(-1)) \
            .withColumn("ArrTimeBlkStart", when(length(df_duplicate.ArrTimeBlk) == 9, substring(df_duplicate.ArrTimeBlk, 1, 4).cast("int")).otherwise(-1)) \
            .withColumn("ArrTimeBlkEnd", when(length(df_duplicate.ArrTimeBlk) == 9, substring(df_duplicate.ArrTimeBlk, 6, 4).cast("int")).otherwise(-1))
        
        df_process_date = df_process_date_tmp \
            .withColumn("DepTimeBlkDistance", df_process_date_tmp.DepTimeBlkEnd - df_process_date_tmp.DepTimeBlkStart) \
            .withColumn("ArrTimeBlkDistance", df_process_date_tmp.ArrTimeBlkEnd - df_process_date_tmp.ArrTimeBlkStart)
        
        # Create table if not exists
        spark.sql("create schema if not exists processed_data")
        spark.sql("use processed_data")
        create_table_sql = f"""
            create table if not exists flight_{year} (
                Year int, Quarter int, Month int, DayofMonth int, DayOfWeek int, FlightDate date,
                Marketing_Airline_Network varchar(255), Operated_or_Branded_Code_Share_Partners varchar(255), DOT_ID_Marketing_Airline varchar(255), IATA_Code_Marketing_Airline varchar(255), Flight_Number_Marketing_Airline int,
                Operating_Airline varchar(255), DOT_ID_Operating_Airline varchar(255), IATA_Code_Operating_Airline varchar(255), Tail_Number varchar(255), Flight_Number_Operating_Airline varchar(255),
                OriginAirportID varchar(255), OriginAirportSeqID varchar(255), OriginCityMarketID varchar(255), Origin varchar(255), OriginCityName varchar(255), OriginState varchar(255), OriginStateFips varchar(255), OriginStateName varchar(255), OriginWac varchar(255),
                DestAirportID varchar(255), DestAirportSeqID varchar(255), DestCityMarketID varchar(255), Dest varchar(255), DestCityName varchar(255), DestState varchar(255), DestStateFips varchar(255), DestStateName varchar(255), DestWac varchar(255),
                CRSDepTime int, DepTime int, DepDelay float, DepDelayMinutes float, DepDel15 float, DepartureDelayGroups int, DepTimeBlkStart int, DepTimeBlkEnd int, DepTimeBlk int,
                TaxiOut float, WheelsOff int, WheelsOn int, TaxiIn float,
                CRSArrTime int, ArrTime int, ArrDelay float, ArrDelayMinutes float, ArrDel15 float, ArrivalDelayGroups int, ArrTimeBlkStart int, ArrTimeBlkEnd int, ArrTimeBlk int,
                Cancelled float,
                CRSElapsedTime float, ActualElapsedTime float, AirTime float,
                Distance float, DistanceGroup int,
                CarrierDelay float, WeatherDelay float, NASDelay float, SecurityDelay float, LateAircraftDelay float,
                FirstDepTime int, TotalAddGTime float, LongestAddGTime float
            )
        """
        spark.sql(create_table_sql)
        
        # Cast data type and Insert data
        df_process_date.createOrReplaceTempView("temp_view")
        insert_into_sql = f"""
            insert into table flight_{year} select
            cast(Year as int), cast(Quarter as int), cast(Month as int), cast(DayofMonth as int), cast(DayOfWeek as int), cast(FlightDate as date),
            cast(Marketing_Airline_Network as varchar(255)), cast(Operated_or_Branded_Code_Share_Partners as varchar(255)), cast(DOT_ID_Marketing_Airline as varchar(255)), cast(IATA_Code_Marketing_Airline as varchar(255)), cast(Flight_Number_Marketing_Airline as int),
            cast(Operating_Airline as varchar(255)), cast(DOT_ID_Operating_Airline as varchar(255)), cast(IATA_Code_Operating_Airline as varchar(255)), cast(Tail_Number as varchar(255)), cast(Flight_Number_Operating_Airline as varchar(255)),
            cast(OriginAirportID as varchar(255)), cast(OriginAirportSeqID as varchar(255)), cast(OriginCityMarketID as varchar(255)), cast(Origin as varchar(255)), cast(OriginCityName as varchar(255)), cast(OriginState as varchar(255)), cast(OriginStateFips as varchar(255)), cast(OriginStateName as varchar(255)), cast(OriginWac as varchar(255)),
            cast(DestAirportID as varchar(255)), cast(DestAirportSeqID as varchar(255)), cast(DestCityMarketID as varchar(255)), cast(Dest as varchar(255)), cast(DestCityName as varchar(255)), cast(DestState as varchar(255)), cast(DestStateFips as varchar(255)), cast(DestStateName as varchar(255)), cast(DestWac as varchar(255)),
            cast(CRSDepTime as int), cast(DepTime as int), cast(DepDelay as float), cast(DepDelayMinutes as float), cast(DepDel15 as float), cast(DepartureDelayGroups as int), cast(DepTimeBlkStart as int), cast(DepTimeBlkEnd as int), cast(DepTimeBlkDistance as int),
            cast(TaxiOut as float), cast(WheelsOff as int), cast(WheelsOn as int), cast(TaxiIn as float),
            cast(CRSArrTime as int), cast(ArrTime as int), cast(ArrDelay as float), cast(ArrDelayMinutes as float), cast(ArrDel15 as float), cast(ArrivalDelayGroups as int), cast(ArrTimeBlkStart as int), cast(ArrTimeBlkEnd as int), cast(ArrTimeBlkDistance as int),
            cast(Cancelled as float),
            cast(CRSElapsedTime as float), cast(ActualElapsedTime as float), cast(AirTime as float),
            cast(Distance as float), cast(DistanceGroup as int),
            cast(CarrierDelay as float), cast(WeatherDelay as float), cast(NASDelay as float), cast(SecurityDelay as float), cast(LateAircraftDelay as float),
            cast(FirstDepTime as int), cast(TotalAddGTime as float), cast(LongestAddGTime as float)
            from temp_view
        """
        spark.sql(insert_into_sql)
