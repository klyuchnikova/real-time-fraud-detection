select client_id from {{ ref('stg_clients') }} where client_id = 0
union all
select t.client_id
from {{ ref('stg_transactions') }} t
left join {{ ref('stg_clients') }} c on t.client_id = c.client_id
where c.client_id is null
