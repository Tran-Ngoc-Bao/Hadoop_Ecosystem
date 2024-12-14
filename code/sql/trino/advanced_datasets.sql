create table if not exists hive.datasets.flights_all_year_quarter
with (format = 'parquet')
as select (Year * 10 + Quarter) year_quarter, count(1) cnt from hive.processed_data.flight_2018 group by Year, Quarter
union
select (Year * 10 + Quarter) year_quarter, count(1) cnt from hive.processed_data.flight_2019 group by Year, Quarter
union
select (Year * 10 + Quarter) year_quarter, count(1) cnt from hive.processed_data.flight_2020 group by Year, Quarter
union
select (Year * 10 + Quarter) year_quarter, count(1) cnt from hive.processed_data.flight_2021 group by Year, Quarter
union
select (Year * 10 + Quarter) year_quarter, count(1) cnt from hive.processed_data.flight_2022 group by Year, Quarter;



create table if not exists hive.datasets.flights_all_year_month
with (format = 'parquet')
as select ((Year - 2000) * 100 + Month) year_month, count(1) cnt from hive.processed_data.flight_2018 group by Year, Month
union
select ((Year - 2000) * 100 + Month) year_month, count(1) cnt from hive.processed_data.flight_2019 group by Year, Month
union
select ((Year - 2000) * 100 + Month) year_month, count(1) cnt from hive.processed_data.flight_2020 group by Year, Month
union
select ((Year - 2000) * 100 + Month) year_month, count(1) cnt from hive.processed_data.flight_2021 group by Year, Month
union
select ((Year - 2000) * 100 + Month) year_month, count(1) cnt from hive.processed_data.flight_2022 group by Year, Month;



create table if not exists hive.datasets.marketing_airline_network_all_year
with (format = 'parquet')
as select
t_2018.Marketing_Airline_Network, percentage_2018, percentage_2019, percentage_2020, percentage_2021, percentage_2022
from
(
    select
    s.Marketing_Airline_Network, (cast(count(1) as real) / b.all * 100) percentage_2018
    from hive.processed_data.flight_2018 s,
    (select count(1) all from hive.processed_data.flight_2018) b
    group by s.Marketing_Airline_Network, b.all
) t_2018,
(
    select
    s.Marketing_Airline_Network, (cast(count(1) as real) / b.all * 100) percentage_2019
    from hive.processed_data.flight_2019 s,
    (select count(1) all from hive.processed_data.flight_2019) b
    group by s.Marketing_Airline_Network, b.all
) t_2019,
(
    select
    s.Marketing_Airline_Network, (cast(count(1) as real) / b.all * 100) percentage_2020
    from hive.processed_data.flight_2020 s,
    (select count(1) all from hive.processed_data.flight_2020) b
    group by s.Marketing_Airline_Network, b.all
) t_2020,
(
    select
    s.Marketing_Airline_Network, (cast(count(1) as real) / b.all * 100) percentage_2021
    from hive.processed_data.flight_2021 s,
    (select count(1) all from hive.processed_data.flight_2021) b
    group by s.Marketing_Airline_Network, b.all
) t_2021,
(
    select
    s.Marketing_Airline_Network, (cast(count(1) as real) / b.all * 100) percentage_2022
    from hive.processed_data.flight_2022 s,
    (select count(1) all from hive.processed_data.flight_2022) b
    group by s.Marketing_Airline_Network, b.all
) t_2022
where
t_2018.Marketing_Airline_Network = t_2019.Marketing_Airline_Network
and t_2018.Marketing_Airline_Network = t_2020.Marketing_Airline_Network
and t_2018.Marketing_Airline_Network = t_2021.Marketing_Airline_Network
and t_2018.Marketing_Airline_Network = t_2022.Marketing_Airline_Network;



create table if not exists hive.datasets.flight_origins_all_year
with (format = 'parquet')
as select
t_2018.Origin, t_2018.OriginCityName, cnt_2018, cnt_2019, cnt_2020, cnt_2021, cnt_2022
from
(
    select
    Year, Origin, OriginCityName, count(1) cnt_2018
    from hive.processed_data.flight_2018
    group by Year, Origin, OriginCityName
) t_2018,
(
    select
    Year, Origin, OriginCityName, count(1) cnt_2019
    from hive.processed_data.flight_2019
    group by Year, Origin, OriginCityName
) t_2019,
(
    select
    Year, Origin, OriginCityName, count(1) cnt_2020
    from hive.processed_data.flight_2020
    group by Year, Origin, OriginCityName
) t_2020,
(
    select
    Year, Origin, OriginCityName, count(1) cnt_2021
    from hive.processed_data.flight_2021
    group by Year, Origin, OriginCityName
) t_2021,
(
    select
    Year, Origin, OriginCityName, count(1) cnt_2022
    from hive.processed_data.flight_2022
    group by Year, Origin, OriginCityName
) t_2022
where
t_2018.Origin = t_2019.Origin
and t_2018.Origin = t_2020.Origin
and t_2018.Origin = t_2021.Origin
and t_2018.Origin = t_2022.Origin;