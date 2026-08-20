select
    date(pickup_ts) as trip_date,
    count(*) as trip_count,
    round(sum(total_amount), 2) as total_revenue,
    round(avg(temperature_2m), 1) as avg_temp_c,
    round(sum(precipitation), 1) as total_precip_mm
from {{ ref('int_trips_weather') }}
group by date(pickup_ts)
order by trip_date