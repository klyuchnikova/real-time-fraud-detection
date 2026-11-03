with expected as (
    select count() as n, sum(amount) as amount from {{ source('raw', 'transactions') }}
)
{% for model in ['mart_daily_state_metrics', 'mart_fraud_by_category', 'mart_fraud_by_state', 'mart_customer_risk_profile'] %}
select '{{ model }}' as model_name, actual.n, actual.amount
from (
    select sum(transaction_count) as n, sum(total_amount) as amount from {{ ref(model) }}
) as actual cross join expected
where actual.n != expected.n or actual.amount != expected.amount
{% if not loop.last %} union all {% endif %}
{% endfor %}
