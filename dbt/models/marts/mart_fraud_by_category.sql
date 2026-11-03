{{ config(order_by='category') }}
select
    category,
    count() as transaction_count,
    sum(toUInt64(target)) as fraud_count,
    100.0 * sum(toUInt64(target)) / count() as fraud_rate,
    sum(amount) as total_amount,
    sumIf(amount, target = 1) as fraud_amount
from {{ ref('stg_transactions') }}
group by category
