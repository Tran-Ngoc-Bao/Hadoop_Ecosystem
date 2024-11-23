from pyspark.sql import SparkSession

def check_leap_year(y):
    yi = int(y)
    if yi % 4:
        return False
    if yi % 400 == 0:
        return True
    if yi % 100 == 0:
        return False
    return True

def increase_time(y, m, d):
    yi = int(y)
    mi = int(m)
    di = int(d)
    if m == "12" and d == "31":
        return str(yi + 1) + " 1 1"
    if d == "31":
        return y + " " + str(mi + 1) + " 1"
    if m == "2" and (d == "29" or (check_leap_year(yi) == False and d == "28")):
        return y + " 3 1"
    if d == "30" and mi in [4, 6, 9, 11]:
        return y + " " + str(mi + 1) + " 1"
    return y + " " + m + " " + str(di + 1)

def solution():
    f = open("/opt/airflow/source/time.txt", "r")
    s = f.read().split(" ")
    f.close()
    year = s[0]
    month = s[1]
    day = s[2]

    df = spark.read.parquet("/opt/airflow/source/flight_data/added_key/" + year + "/" + month + "/" + day + ".parquet")
    df_reference_data = df.select("Origin", "OriginCityname", "OriginState", "OriginStateName", "Dest", "DestCityName", "DestState", "DestStateName", "FlightDate")
    df_transaction_data = df.drop("OriginCityname", "OriginStateName", "DestCityName", "DestStateName")
    df_reference_data.repartition(1).write.mode("overwrite").parquet("hdfs://namenode:9000/staging/reference/" + year + "/" + month + "/" + day)
    df_transaction_data.repartition(1).write.mode("overwrite").parquet("hdfs://namenode:9000/staging/transaction/" + year + "/" + month + "/" + day)

    f = open("/opt/airflow/source/time.txt", "w")
    f.write(increase_time(year, month, day))
    f.close()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Push file").getOrCreate()
    flag = True
    while flag:
        try:
            solution()
        except:
            print("Don't worry about this error")
            flag = False
    # solution()