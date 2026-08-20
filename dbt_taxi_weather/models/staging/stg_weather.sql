select
    weather_hour_local,
    temperature_2m,
    precipitation,
    snowfall,
    wind_speed_10m,
    weather_code
from {{ source('silver_raw', 'weather_input') }}