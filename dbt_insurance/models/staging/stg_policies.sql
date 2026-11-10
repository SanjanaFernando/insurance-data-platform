select
    policy_id::int                      as policy_id,
    customer_id::int                    as customer_id,
    policy_type,
    try_cast(annual_premium as decimal(10, 2)) as annual_premium
from {{ source('raw', 'policies') }}
