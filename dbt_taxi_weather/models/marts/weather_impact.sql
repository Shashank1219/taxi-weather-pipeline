with enriched as (
    select
        *,
        case when weekday in ('Saturday', 'Sunday') then 'weekend' else 'weekday' end as day_type,
        case
            when temperature_2m < 0 then 'freezing'
            when temperature_2m < 10 then 'cold'
            when temperature_2m < 20 then 'mild'
            else 'warm'
        end as temp_bucket
    from {{ ref('int_trips_weather') }}
)
select
    day_type,
    is_precipitation,
    temp_bucket,
    count(*) as total_trips,
    count(distinct pickup_hour) as distinct_hours,
    round(count(*) / count(distinct pickup_hour), 1) as avg_trips_per_hour
from enriched
group by day_type, is_precipitation, temp_bucket
order by day_type, is_precipitation, temp_bucket