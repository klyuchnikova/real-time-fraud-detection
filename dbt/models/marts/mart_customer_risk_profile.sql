{{ config(order_by='client_id') }}
with history as (
    select
        client_id,
        min(transaction_time) as first_transaction_at,
        max(transaction_time) as last_transaction_at,
        count() as transaction_count,
        sum(toUInt64(target)) as fraud_count,
        100.0 * sum(toUInt64(target)) / count() as fraud_rate,
        sum(amount) as total_amount,
        sumIf(amount, target = 1) as fraud_amount,
        avg(amount) as average_amount
    from {{ ref('stg_transactions') }}
    group by client_id
)
select
    h.client_id,
    c.state_code,
    h.first_transaction_at,
    h.last_transaction_at,
    h.transaction_count,
    h.fraud_count,
    h.fraud_rate,
    h.total_amount,
    h.fraud_amount,
    h.average_amount,
    {{ customer_risk('h.fraud_rate') }} as risk_level
from history as h
inner join {{ ref('stg_clients') }} as c on h.client_id = c.client_id
