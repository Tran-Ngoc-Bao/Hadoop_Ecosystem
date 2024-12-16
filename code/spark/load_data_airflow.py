from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType
import threading
from sys import argv

if __name__ == "__main__":
    year = int(argv[1])
    month = int(argv[2])
    
    spark = SparkSession.builder \
        .appName("Load data") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1') \
        .getOrCreate()

    if (year == 2022 and month >= 8) or year > 2022:
        pass
    else:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker01:9093") \
            .option("subscribe", f"flight_data_{year}") \
            .load()
        
        json_df = df.selectExpr("CAST(key AS STRING) as msg_key", "CAST(value AS STRING) as msg_value")

        json_schema = StructType([
            StructField('Year', StringType(), True),
            StructField('Quarter', StringType(), True),
            StructField('Month', StringType(), True),
            StructField('DayofMonth', StringType(), True),
            StructField('DayOfWeek', StringType(), True),
            StructField('FlightDate', StringType(), True),
            StructField('Marketing_Airline_Network', StringType(), True),
            StructField('Operated_or_Branded_Code_Share_Partners', StringType(), True),
            StructField('DOT_ID_Marketing_Airline', StringType(), True),
            StructField('IATA_Code_Marketing_Airline', StringType(), True),
            StructField('Flight_Number_Marketing_Airline	', StringType(), True),
            StructField('Originally_Scheduled_Code_Share_Airline', StringType(), True),
            StructField('DOT_ID_Originally_Scheduled_Code_Share_Airline', StringType(), True),
            StructField('IATA_Code_Originally_Scheduled_Code_Share_Airline', StringType(), True),
            StructField('Flight_Num_Originally_Scheduled_Code_Share_Airline', StringType(), True),
            StructField('Operating_Airline', StringType(), True),
            StructField('DOT_ID_Operating_Airline', StringType(), True),
            StructField('IATA_Code_Operating_Airline', StringType(), True),
            StructField('Tail_Number', StringType(), True),
            StructField('Flight_Number_Operating_Airline', StringType(), True),
            StructField('OriginAirportID', StringType(), True),
            StructField('OriginAirportSeqID', StringType(), True),
            StructField('OriginCityMarketID', StringType(), True),
            StructField('Origin', StringType(), True),
            StructField('OriginCityName', StringType(), True),
            StructField('OriginState', StringType(), True),
            StructField('OriginStateFips', StringType(), True),
            StructField('OriginStateName', StringType(), True),
            StructField('OriginWac', StringType(), True),
            StructField('DestAirportID', StringType(), True),
            StructField('DestAirportSeqID', StringType(), True),
            StructField('DestCityMarketID', StringType(), True),
            StructField('Dest', StringType(), True),
            StructField('DestCityName', StringType(), True),
            StructField('DestState', StringType(), True),
            StructField('DestStateFips', StringType(), True),
            StructField('DestStateName', StringType(), True),
            StructField('DestWac', StringType(), True),
            StructField('CRSDepTime', StringType(), True),
            StructField('DepTime', StringType(), True),
            StructField('DepDelay', StringType(), True),
            StructField('DepDelayMinutes', StringType(), True),
            StructField('DepDel15', StringType(), True),
            StructField('DepartureDelayGroups', StringType(), True),
            StructField('DepTimeBlk', StringType(), True),
            StructField('TaxiOut', StringType(), True),
            StructField('WheelsOff', StringType(), True),
            StructField('WheelsOn', StringType(), True),
            StructField('TaxiIn', StringType(), True),
            StructField('CRSArrTime', StringType(), True),
            StructField('ArrTime', StringType(), True),
            StructField('ArrDelay', StringType(), True),
            StructField('ArrDelayMinutes', StringType(), True),
            StructField('ArrDel15', StringType(), True),
            StructField('ArrDelayGroups', StringType(), True),
            StructField('ArrTimeBlk', StringType(), True),
            StructField('Cancelled', StringType(), True),
            StructField('CancellationCode', StringType(), True),
            StructField('Diverted', StringType(), True),
            StructField('CRSElapsedTime', StringType(), True),
            StructField('ActualElapsedTime', StringType(), True),
            StructField('AirTime', StringType(), True),
            StructField('Flights', StringType(), True),
            StructField('Distance', StringType(), True),
            StructField('DistanceGroup', StringType(), True),
            StructField('CarrierDelay', StringType(), True),
            StructField('WeatherDelay', StringType(), True),
            StructField('NASDelay', StringType(), True),
            StructField('SecurityDelay', StringType(), True),
            StructField('LateAircraftDelay', StringType(), True),
            StructField('FirstDepTime', StringType(), True),
            StructField('TotalAddGTime', StringType(), True),
            StructField('LongestAddGTime', StringType(), True),
            StructField('DivAirportLandings	', StringType(), True),
            StructField('DivReachedDest', StringType(), True),
            StructField('DivActualElapsedTime', StringType(), True),
            StructField('DivArrDelay', StringType(), True),
            StructField('DivDistance', StringType(), True),
            StructField('Div1Airport', StringType(), True),
            StructField('Div1AirportID', StringType(), True),
            StructField('Div1AirportSeqID', StringType(), True),
            StructField('Div1WheelsOn', StringType(), True),
            StructField('Div1TotalGTime', StringType(), True),
            StructField('Div1LongestGTime', StringType(), True),
            StructField('Div1WheelsOff', StringType(), True),
            StructField('Div1TailNum', StringType(), True),
            StructField('Div2Airport', StringType(), True),
            StructField('Div2AirportID', StringType(), True),
            StructField('Div2AirportSeqID', StringType(), True),
            StructField('Div2WheelsOn', StringType(), True),
            StructField('Div2TotalGTime', StringType(), True),
            StructField('Div2LongestGTime', StringType(), True),
            StructField('Div2WheelsOff', StringType(), True),
            StructField('Div2TailNum', StringType(), True),
            StructField('Div3Airport', StringType(), True),
            StructField('Div3AirportID', StringType(), True),
            StructField('Div3AirportSeqID', StringType(), True),
            StructField('Div3WheelsOn', StringType(), True),
            StructField('Div3TotalGTime', StringType(), True),
            StructField('Div3LongestGTime', StringType(), True),
            StructField('Div3WheelsOff', StringType(), True),
            StructField('Div3TailNum', StringType(), True),
            StructField('Div4Airport', StringType(), True),
            StructField('Div4AirportID', StringType(), True),
            StructField('Div4AirportSeqID', StringType(), True),
            StructField('Div4WheelsOn', StringType(), True),
            StructField('Div4TotalGTime', StringType(), True),
            StructField('Div4LongestGTime', StringType(), True),
            StructField('Div4WheelsOff', StringType(), True),
            StructField('Div4TailNum', StringType(), True),
            StructField('Div5Airport', StringType(), True),
            StructField('Div5AirportID', StringType(), True),
            StructField('Div5AirportSeqID', StringType(), True),
            StructField('Div5WheelsOn', StringType(), True),
            StructField('Div5TotalGTime', StringType(), True),
            StructField('Div5LongestGTime', StringType(), True),
            StructField('Div5WheelsOff', StringType(), True),
            StructField('Div5TailNum', StringType(), True),
            StructField('Duplicate', StringType(), True)
        ])

        json_expanded_df = json_df.withColumn("msg_value", from_json(json_df["msg_value"], json_schema)).select("msg_value.*")
        
        writing_df = json_expanded_df.writeStream \
            .format("parquet") \
            .option("format", "append") \
            .option("path", "hdfs://namenode:9000/staging/" + str(year) + "/" + str(month)) \
            .option("checkpointLocation", "hdfs://namenode:9000/tmp/" + str(year) + "/" + str(month)) \
            .outputMode("append") \
            .start()
        
        def stop_query():
            writing_df.stop()

        timer = threading.Timer(10 * 60, stop_query)
        timer.start()

        writing_df.awaitTermination()
