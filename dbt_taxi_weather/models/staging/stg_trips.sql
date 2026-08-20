select
    VendorID as vendor_id,
    cast(tpep_pickup_datetime as timestamp) as pickup_ts,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_ts,
    passenger_count,
    trip_distance,
    PULocationID as pickup_location_id,
    DOLocationID as dropoff_location_id,
    payment_type,
    fare_amount,
    tip_amount,
    total_amount
from {{ source('silver_raw', 'trips_input') }}