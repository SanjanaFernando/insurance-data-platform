-- Do rainy days (>= 10 mm) see more claims than dry days?
select
    is_rainy_day,
    count(*)                                  as days,
    round(avg(claims), 2)                     as avg_claims_per_day,
    round(avg(claim_amount), 2)               as avg_claim_amount_per_day
from {{ ref('mart_claims_daily') }}
group by is_rainy_day
