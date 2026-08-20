with trips as (
    select *, date_trunc('hour', pickup_ts) as pickup_hour
    from {{ ref('stg_trips') }}
),
weather as (
    select * from {{ ref('stg_weather') }}
)
select
    trips.*,
    weather.temperature_2m,
    weather.precipitation,
    weather.snowfall,
    weather.wind_speed_10m,
    weather.weather_code,
    (unix_timestamp(trips.dropoff_ts) - unix_timestamp(trips.pickup_ts)) / 60 as trip_minutes,
    date_format(trips.pickup_ts, 'EEEE') as weekday,
    case when weather.precipitation > 0.1 then true else false end as is_precipitation
from trips
left join weather
    on trips.pickup_hour = weather.weather_hour_local