with matched as (
    select
        count(*) as total,
        count(temperature_2m) as matched_rows
    from {{ ref('int_trips_weather') }}
)
select *
from matched
where matched_rows < total * {{ var('weather_match_rate_threshold') }}