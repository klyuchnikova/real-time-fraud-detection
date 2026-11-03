{% for model in ['mart_fraud_by_category', 'mart_fraud_by_state', 'mart_customer_risk_profile'] %}
select '{{ model }}' as model_name, fraud_rate, fraud_count, transaction_count
from {{ ref(model) }}
where not isFinite(fraud_rate) or fraud_rate < 0 or fraud_rate > 100
    or fraud_count > transaction_count
    or abs(fraud_rate - 100.0 * fraud_count / transaction_count) > 0.000000001
{% if not loop.last %} union all {% endif %}
{% endfor %}
