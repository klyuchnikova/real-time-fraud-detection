-- depends_on: {{ ref('stg_transactions') }}
with cases as (
    select tupleElement(item, 1) as amount, tupleElement(item, 2) as expected
    from (
        select arrayJoin([
            (toDecimal64('0',2), 'SMALL'),
            (toDecimal64('49.99',2), 'SMALL'),
            (toDecimal64('50',2), 'MEDIUM'),
            (toDecimal64('199.99',2), 'MEDIUM'),
            (toDecimal64('200',2), 'LARGE'),
            (toDecimal64({{ var('large_amount_threshold') }},2)-toDecimal64('0.01',2), 'LARGE'),
            (toDecimal64({{ var('large_amount_threshold') }},2), 'VERY_LARGE')
        ]) as item
    )
)
select amount, expected, {{ amount_bucket('amount') }} as actual
from cases where actual != expected
