select
    cast(LocationID as int) as location_id,
    Borough as borough,
    Zone as zone,
    service_zone
from {{ source('silver_raw', 'zone_lookup') }}