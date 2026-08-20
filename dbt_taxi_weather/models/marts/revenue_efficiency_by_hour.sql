select
    hour(pickup_hour) as hour_of_day,
    round(avg(trip_minutes), 1) as avg_trip_minutes,
    round(avg(case when trip_minutes > 0 then total_amount / trip_minutes else null end), 2) as avg_revenue_per_minute,
    round(sum(total_amount), 0) as total_revenue,
    count(*) as trip_count
from {{ ref('int_trips_weather') }}
where trip_minutes > 0 and trip_minutes < 180
group by hour(pickup_hour)
order by hour_of_day