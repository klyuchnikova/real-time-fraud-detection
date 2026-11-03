select transaction_id as record_id, 'calendar' as failed_rule
from {{ ref('stg_transactions') }}
where day_of_week != toDayOfWeek(transaction_time) or transaction_date != toDate(transaction_time, 'UTC')
union all
select toUInt64(0) as record_id, 'share_or_p95' as failed_rule
from {{ ref('mart_daily_state_metrics') }}
where large_transaction_share < 0 or large_transaction_share > 1
    or not isFinite(average_amount) or average_amount < 0 or p95_amount < 0
