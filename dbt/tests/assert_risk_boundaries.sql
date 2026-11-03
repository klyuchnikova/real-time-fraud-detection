-- depends_on: {{ ref('mart_customer_risk_profile') }}
with cases as (
    select tupleElement(item,1) as rate, tupleElement(item,2) as expected
    from (
        select arrayJoin([
            (toFloat64(0), 'LOW'),
            (toFloat64({{ var('risk_medium_pct') }})-0.000001, 'LOW'),
            (toFloat64({{ var('risk_medium_pct') }}), 'MEDIUM'),
            (toFloat64({{ var('risk_high_pct') }})-0.000001, 'MEDIUM'),
            (toFloat64({{ var('risk_high_pct') }}), 'HIGH'),
            (toFloat64(100), 'HIGH')
        ]) as item
    )
)
select rate, expected, {{ customer_risk('rate') }} as actual
from cases where actual != expected
