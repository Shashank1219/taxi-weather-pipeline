select
    rejection_reason,
    tpep_pickup_datetime as pickup_ts,
    trip_distance,
    total_amount
from {{ source('silver_raw', 'quarantine') }}