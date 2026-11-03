select
    transaction_id,
    client_id,
    toDateTime(transaction_time, 'UTC') as transaction_time,
    toDate(transaction_time, 'UTC') as transaction_date,
    toUInt8({{ dbt_date.day_of_week('transaction_time', isoweek=true) }}) as day_of_week,
    trimBoth(merch) as merchant,
    trimBoth(cat_id) as category,
    toDecimal64(amount, 2) as amount,
    upperUTF8(trimBoth(gender)) as gender,
    upperUTF8(trimBoth(us_state)) as state_code,
    toUInt8(target) as target,
    {{ amount_bucket('amount') }} as amount_bucket,
    toUInt8(amount >= {{ var('large_amount_threshold', 1000) }}) as is_large_transaction
from {{ source('raw', 'transactions') }}
