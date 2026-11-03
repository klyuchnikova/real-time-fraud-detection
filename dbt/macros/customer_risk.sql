{% macro customer_risk(fraud_rate_column) -%}
    case
        when {{ fraud_rate_column }} >= {{ var('risk_high_pct', 5) }} then 'HIGH'
        when {{ fraud_rate_column }} >= {{ var('risk_medium_pct', 1) }} then 'MEDIUM'
        else 'LOW'
    end
{%- endmacro %}
