{% macro validate_business_rules() %}
    {% if var('large_amount_threshold') <= 200 %}
        {{ exceptions.raise_compiler_error('large_amount_threshold must exceed 200') }}
    {% endif %}
    {% if not (0 < var('risk_medium_pct') < var('risk_high_pct') <= 100) %}
        {{ exceptions.raise_compiler_error('Risk thresholds must satisfy 0 < medium < high <= 100') }}
    {% endif %}
    {{ return('select 1') }}
{% endmacro %}
