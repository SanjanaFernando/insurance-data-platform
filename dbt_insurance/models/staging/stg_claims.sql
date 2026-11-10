select
    claim_id::int                              as claim_id,
    policy_id::int                             as policy_id,
    try_cast(claim_date as date)               as claim_date,
    try_cast(claim_amount as decimal(12, 2))   as claim_amount,
    coalesce(upper(trim(status)), 'UNKNOWN')   as status
from {{ source('raw', 'claims') }}
where claim_id is not null
  and try_cast(claim_date as date) is not null
  and try_cast(claim_amount as decimal(12, 2)) > 0
qualify row_number() over (partition by claim_id order by try_cast(claim_date as date) desc) = 1
