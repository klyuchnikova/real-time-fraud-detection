select
    toUInt64(client_id) as client_id,
    trimBoth(name_1) as first_name,
    trimBoth(name_2) as last_name,
    trimBoth(street) as street,
    trimBoth(one_city) as city,
    upperUTF8(trimBoth(us_state)) as state_code,
    trimBoth(post_code) as post_code
from {{ source('raw', 'clients') }}
