select
        case payment_type
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
        when 0 then 'Undocumented code (0)'
        else 'Other'
    end as payment_type_label,
    count(*) as trip_count,
    round(avg(total_amount), 2) as avg_fare,
    round(avg(tip_amount), 2) as avg_tip,
    round(avg(case when fare_amount > 0 then tip_amount / fare_amount else null end) * 100, 1) as avg_tip_pct
from {{ ref('int_trips_weather') }}
group by 1
order by trip_count desc