from pyspark.sql import SparkSession

if __name__ == "__main__":
    datawarehouse_location = 'hdfs://namenode:9000/datawarehouse'
    spark = SparkSession.builder.appName("Create processed data storage").config("spark.sql.warehouse.dir", datawarehouse_location).enableHiveSupport().getOrCreate()
    
    spark.sql("create schema if not exists data_warehouse")
    spark.sql("use data_warehouse")

    spark.sql("""
        create table if not exists flight (
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
    """)
