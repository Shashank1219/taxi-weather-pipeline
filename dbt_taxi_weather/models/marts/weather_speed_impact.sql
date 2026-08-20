with speed_calc as (
    select
        *,
        case when trip_minutes > 0 then trip_distance / (trip_minutes / 60.0) else null end as speed_mph,
        case when weekday in ('Saturday', 'Sunday') then 'weekend' else 'weekday' end as day_type,
        case
            when temperature_2m < 0 then 'freezing'
            when temperature_2m < 10 then 'cold'
            when temperature_2m < 20 then 'mild'
            else 'warm'
        end as temp_bucket
    from {{ ref('int_trips_weather') }}
    where trip_minutes between 1 and 180 and trip_distance > 0
)
select
    is_precipitation,
    temp_bucket,
    day_type,
    round(avg(speed_mph), 1) as avg_speed_mph,
    count(*) as trip_count
from speed_calc
where speed_mph < 60
group by is_precipitation, temp_bucket, day_type
order by day_type, temp_bucket, is_precipitation