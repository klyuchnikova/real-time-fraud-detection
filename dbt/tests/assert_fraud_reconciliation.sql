with expected as (
    select sum(toUInt64(target)) as frauds, sumIf(amount, target=1) as amount
    from {{ source('raw', 'transactions') }}
)
{% for model in ['mart_fraud_by_category', 'mart_fraud_by_state', 'mart_customer_risk_profile'] %}
select '{{ model }}' as model_name, actual.frauds, actual.amount
from (
    select sum(fraud_count) as frauds, sum(fraud_amount) as amount from {{ ref(model) }}
) as actual cross join expected
where actual.frauds != expected.frauds or actual.amount != expected.amount
{% if not loop.last %} union all {% endif %}
{% endfor %}
