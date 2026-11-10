-- One row per calendar day: weather plus claim activity (days with no claims show 0).
with claims_by_day as (
    select claim_date, count(*) as claims, sum(claim_amount) as claim_amount
    from {{ ref('stg_claims') }}
    group by claim_date
)
select
    w.weather_date,
    w.temp_max_c,
    w.precip_mm,
    (w.precip_mm >= 10)                    as is_rainy_day,
    coalesce(c.claims, 0)                  as claims,
    coalesce(c.claim_amount, 0)            as claim_amount
from {{ ref('stg_weather') }} w
left join claims_by_day c on c.claim_date = w.weather_date
