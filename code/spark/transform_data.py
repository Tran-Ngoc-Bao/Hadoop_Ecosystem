from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def solution():
    global year
    global month

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
    
    # Cast data type and Insert data
    df_process_date.createOrReplaceTempView("temp_view")
    spark.sql("""
              insert into table flight select
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
              """)

    if month == 12:
        year += 1
        month = 1
    else:
        month += 1

if __name__ == "__main__":
    datawarehouse_location = 'hdfs://namenode:9000/datawarehouse'
    spark = SparkSession.builder.appName("Transform data").config("spark.sql.warehouse.dir", datawarehouse_location).enableHiveSupport().getOrCreate()
    spark.sql("use data_warehouse")

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
