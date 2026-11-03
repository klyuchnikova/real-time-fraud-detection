{% macro amount_bucket(column_name) -%}
    case
        when {{ column_name }} < 50 then 'SMALL'
        when {{ column_name }} < 200 then 'MEDIUM'
        when {{ column_name }} < {{ var('large_amount_threshold', 1000) }} then 'LARGE'
        else 'VERY_LARGE'
    end
{%- endmacro %}
