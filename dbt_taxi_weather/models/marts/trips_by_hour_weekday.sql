select
    hour(pickup_hour) as hour_of_day,
    weekday,
    count(*) as trip_count,
    round(avg(total_amount), 2) as avg_fare,
    round(avg(trip_minutes), 1) as avg_trip_minutes
from {{ ref('int_trips_weather') }}
group by hour(pickup_hour), weekday