{{ config(order_by='(transaction_date, state_code)') }}
select
    transaction_date,
    state_code,
    count() as transaction_count,
    sum(amount) as total_amount,
    avg(amount) as average_amount,
    quantileExact(0.95)(amount) as p95_amount,
    countIf(is_large_transaction = 1) / toFloat64(count()) as large_transaction_share
from {{ ref('stg_transactions') }}
group by transaction_date, state_code
