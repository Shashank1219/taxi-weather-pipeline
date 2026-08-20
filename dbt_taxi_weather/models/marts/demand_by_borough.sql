select
    pickup_borough,
    hour(pickup_hour) as hour_of_day,
    count(*) as trip_count,
    round(avg(total_amount), 2) as avg_fare,
    round(sum(total_amount), 2) as total_revenue
from {{ ref('int_trips_weather') }}
where pickup_borough is not null
group by pickup_borough, hour(pickup_hour)
order by pickup_borough, hour_of_day