{{ config(order_by='state_code') }}
select
    state_code,
    count() as transaction_count,
    sum(toUInt64(target)) as fraud_count,
    100.0 * sum(toUInt64(target)) / count() as fraud_rate,
    uniqExact(client_id) as unique_clients,
    uniqExact(merchant) as unique_merchants,
    sum(amount) as total_amount,
    sumIf(amount, target = 1) as fraud_amount
from {{ ref('stg_transactions') }}
group by state_code
