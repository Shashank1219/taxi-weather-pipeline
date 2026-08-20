select
    coalesce(date_format(pickup_ts, 'yyyy-MM'), 'unknown_month') as trip_month,
    rejection_reason,
    count(*) as quarantined_count
from {{ ref('stg_quarantine') }}
group by coalesce(date_format(pickup_ts, 'yyyy-MM'), 'unknown_month'), rejection_reason
order by trip_month, rejection_reason