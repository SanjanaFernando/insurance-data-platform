select
    try_cast(weather_date as date)   as weather_date,
    try_cast(temp_max_c as double)   as temp_max_c,
    try_cast(precip_mm as double)    as precip_mm
from {{ source('raw', 'weather_daily') }}
where try_cast(weather_date as date) is not null
qualify row_number() over (partition by try_cast(weather_date as date) order by weather_date) = 1
