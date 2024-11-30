create table if not exists hive.datasets.cancellation_code_2019
with (format = 'parquet')
as select
Year, Quarter, Month,
count_if(CarrierDelay > 0) carrier_delay,
count_if(WeatherDelay > 0) weather_delay,
count_if(NASDelay > 0) nas_delay,
count_if(SecurityDelay > 0) security_delay,
count_if(LateAircraftDelay > 0) late_aircraft_delay
from hive.processed_data.flight_2019 
group by Year, Month, Quarter;



create table if not exists hive.datasets.marketing_airline_network_2019
with (format = 'parquet')
as select
Year, Quarter, Month,
Marketing_Airline_Network, count(1) cnt
from hive.processed_data.flight_2019
group by Year, Quarter, Month, Marketing_Airline_Network;



create table if not exists hive.datasets.flight_distances_2019
with (format = 'parquet')
as select 
Year, Quarter, Month,
count_if(distance < 250) d_0_250,
count_if(distance >= 250 and distance < 500) d_250_500,
count_if(distance >= 500 and distance < 750) d_500_750,
count_if(distance >= 750 and distance < 1000) d_750_1000,
count_if(distance >= 1000 and distance < 1250) d_1000_1250,
count_if(distance >= 1250 and distance < 1500) d_1250_1500,
count_if(distance >= 1500 and distance < 1750) d_1500_1750,
count_if(distance >= 1750 and distance < 2000) d_1750_2000,
count_if(distance >= 2000 and distance < 2250) d_2000_2250,
count_if(distance >= 2250 and distance < 2500) d_2250_2500,
count_if(distance >= 2500 and distance < 2750) d_2500_2750,
count_if(distance >= 2750 and distance < 3000) d_2750_3000,
count_if(distance >= 3000 and distance < 3250) d_3000_3250,
count_if(distance >= 3250 and distance < 3500) d_3250_3500,
count_if(distance >= 3500 and distance < 3750) d_3500_3750,
count_if(distance >= 3750 and distance < 4000) d_3750_4000,
count_if(distance >= 4000 and distance < 4250) d_4000_4250,
count_if(distance >= 4250 and distance < 4500) d_4250_4500,
count_if(distance >= 4500 and distance < 4750) d_4500_4750,
count_if(distance >= 4750 and distance < 5000) d_4750_5000
from hive.processed_data.flight_2019
group by Year, Quarter, Month;



create table if not exists hive.datasets.flights_across_days_of_the_week_2019
with (format = 'parquet')
as select
Year, Quarter, Month, DayOfWeek,
count(1) cnt
from hive.processed_data.flight_2019
group by Year, Quarter, Month, DayOfWeek;



create table if not exists hive.datasets.departure_times_2019
with (format = 'parquet')
as select
Year, Quarter, Month,
count_if(DepTime < 100) dt_0000_0100,
count_if(DepTime >= 100 and DepTime < 200) dt_0100_0200,
count_if(DepTime >= 200 and DepTime < 300) dt_0200_0300,
count_if(DepTime >= 300 and DepTime < 400) dt_0300_0400,
count_if(DepTime >= 400 and DepTime < 500) dt_0400_0500,
count_if(DepTime >= 500 and DepTime < 600) dt_0500_0600,
count_if(DepTime >= 600 and DepTime < 700) dt_0600_0700,
count_if(DepTime >= 700 and DepTime < 800) dt_0700_0800,
count_if(DepTime >= 800 and DepTime < 900) dt_0800_0900,
count_if(DepTime >= 900 and DepTime < 1000) dt_0900_1000,
count_if(DepTime >= 1000 and DepTime < 1100) dt_1000_1100,
count_if(DepTime >= 1100 and DepTime < 1200) dt_1100_1200,
count_if(DepTime >= 1200 and DepTime < 1300) dt_1200_1300,
count_if(DepTime >= 1300 and DepTime < 1400) dt_1300_1400,
count_if(DepTime >= 1400 and DepTime < 1500) dt_1400_1500,
count_if(DepTime >= 1500 and DepTime < 1600) dt_1500_1600,
count_if(DepTime >= 1600 and DepTime < 1700) dt_1600_1700,
count_if(DepTime >= 1700 and DepTime < 1800) dt_1700_1800,
count_if(DepTime >= 1800 and DepTime < 1900) dt_1800_1900,
count_if(DepTime >= 1900 and DepTime < 2000) dt_1900_2000,
count_if(DepTime >= 2000 and DepTime < 2100) dt_2000_2100,
count_if(DepTime >= 2100 and DepTime < 2200) dt_2100_2200,
count_if(DepTime >= 2200 and DepTime < 2300) dt_2200_2300,
count_if(DepTime >= 2300 and DepTime < 2400) dt_2300_2400
from hive.processed_data.flight_2019
group by Year, Quarter, Month;



create table if not exists hive.datasets.flight_origins_2019
with (format = 'parquet')
as select
Year, Quarter, Month,
Origin, OriginCityName, count(1) cnt
from hive.processed_data.flight_2019
group by Year, Quarter, Month, Origin, OriginCityName;



create table if not exists hive.datasets.flights_cancellations_by_day_of_the_week_2019
with (format = 'parquet')
as select
Year, Quarter, Month, DayOfWeek,
(cast(count_if(Cancelled = 1) as real) / count(1) * 100) percentage
from hive.processed_data.flight_2019
group by Year, Quarter, Month, DayOfWeek;



create table if not exists hive.datasets.total_cancellations_and_flights_per_carrier_2019
with (format = 'parquet')
as select
s.Year, s.Quarter, s.Month,
s.Marketing_Airline_Network,
(cast(count(1) as real) / b.total_flights * 100) percentage_flights, (cast(count_if(Cancelled = 1) as real) / b.total_cancellations * 100) percentage_cancellations
from hive.processed_data.flight_2019 s,
(select Year, Quarter, Month, count(1) total_flights, count_if(Cancelled = 1) total_cancellations from hive.processed_data.flight_2019 group by Year, Quarter, Month) b
where s.Year = b.Year and s.Quarter = b.Quarter and s.Month = b.Month
group by s.Year, s.Quarter, s.Month, s.Marketing_Airline_Network, b.total_flights, b.total_cancellations;



create table if not exists hive.datasets.day_of_month_2019
with (format = 'parquet')
as select
Year, Quarter, Month,
(cast(count_if(DayofMonth = 1) as real) / count(1) * 100) all_1, (cast(count_if(DayofMonth = 1 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_1,
(cast(count_if(DayofMonth = 2) as real) / count(1) * 100) all_2, (cast(count_if(DayofMonth = 2 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_2,
(cast(count_if(DayofMonth = 3) as real) / count(1) * 100) all_3, (cast(count_if(DayofMonth = 3 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_3,
(cast(count_if(DayofMonth = 4) as real) / count(1) * 100) all_4, (cast(count_if(DayofMonth = 4 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_4,
(cast(count_if(DayofMonth = 5) as real) / count(1) * 100) all_5, (cast(count_if(DayofMonth = 5 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_5,
(cast(count_if(DayofMonth = 6) as real) / count(1) * 100) all_6, (cast(count_if(DayofMonth = 6 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_6,
(cast(count_if(DayofMonth = 7) as real) / count(1) * 100) all_7, (cast(count_if(DayofMonth = 7 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_7,
(cast(count_if(DayofMonth = 8) as real) / count(1) * 100) all_8, (cast(count_if(DayofMonth = 8 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_8,
(cast(count_if(DayofMonth = 9) as real) / count(1) * 100) all_9, (cast(count_if(DayofMonth = 9 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_9,
(cast(count_if(DayofMonth = 10) as real) / count(1) * 100) all_10, (cast(count_if(DayofMonth = 10 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_10,
(cast(count_if(DayofMonth = 11) as real) / count(1) * 100) all_11, (cast(count_if(DayofMonth = 11 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_11,
(cast(count_if(DayofMonth = 12) as real) / count(1) * 100) all_12, (cast(count_if(DayofMonth = 12 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_12,
(cast(count_if(DayofMonth = 13) as real) / count(1) * 100) all_13, (cast(count_if(DayofMonth = 13 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_13,
(cast(count_if(DayofMonth = 14) as real) / count(1) * 100) all_14, (cast(count_if(DayofMonth = 14 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_14,
(cast(count_if(DayofMonth = 15) as real) / count(1) * 100) all_15, (cast(count_if(DayofMonth = 15 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_15,
(cast(count_if(DayofMonth = 16) as real) / count(1) * 100) all_16, (cast(count_if(DayofMonth = 16 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_16,
(cast(count_if(DayofMonth = 17) as real) / count(1) * 100) all_17, (cast(count_if(DayofMonth = 17 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_17,
(cast(count_if(DayofMonth = 18) as real) / count(1) * 100) all_18, (cast(count_if(DayofMonth = 18 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_18,
(cast(count_if(DayofMonth = 19) as real) / count(1) * 100) all_19, (cast(count_if(DayofMonth = 19 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_19,
(cast(count_if(DayofMonth = 20) as real) / count(1) * 100) all_20, (cast(count_if(DayofMonth = 20 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_20,
(cast(count_if(DayofMonth = 21) as real) / count(1) * 100) all_21, (cast(count_if(DayofMonth = 21 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_21,
(cast(count_if(DayofMonth = 22) as real) / count(1) * 100) all_22, (cast(count_if(DayofMonth = 22 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_22,
(cast(count_if(DayofMonth = 23) as real) / count(1) * 100) all_23, (cast(count_if(DayofMonth = 23 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_23,
(cast(count_if(DayofMonth = 24) as real) / count(1) * 100) all_24, (cast(count_if(DayofMonth = 24 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_24,
(cast(count_if(DayofMonth = 25) as real) / count(1) * 100) all_25, (cast(count_if(DayofMonth = 25 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_25,
(cast(count_if(DayofMonth = 26) as real) / count(1) * 100) all_26, (cast(count_if(DayofMonth = 26 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_26,
(cast(count_if(DayofMonth = 27) as real) / count(1) * 100) all_27, (cast(count_if(DayofMonth = 27 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_27,
(cast(count_if(DayofMonth = 28) as real) / count(1) * 100) all_28, (cast(count_if(DayofMonth = 28 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_28,
(cast(count_if(DayofMonth = 29) as real) / count(1) * 100) all_29, (cast(count_if(DayofMonth = 29 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_29,
(cast(count_if(DayofMonth = 30) as real) / count(1) * 100) all_30, (cast(count_if(DayofMonth = 30 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_30,
(cast(count_if(DayofMonth = 31) as real) / count(1) * 100) all_31, (cast(count_if(DayofMonth = 31 and Cancelled = 1) as real) / count_if(Cancelled = 1) * 100) cancelled_31
from hive.processed_data.flight_2019
group by Year, Quarter, Month;



create table if not exists hive.datasets.mean_delay_by_aircraft_carrier_2019
with (format = 'parquet')
as select
Year, Quarter, Month, Marketing_Airline_Network,
avg(CarrierDelay) carrier_delay,
avg(WeatherDelay) weather_delay,
avg(NASDelay) nas_delay,
avg(SecurityDelay) security_delay,
avg(LateAircraftDelay) late_aircraft_delay
from hive.processed_data.flight_2019 
group by Year, Month, Quarter, Marketing_Airline_Network;