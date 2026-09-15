{% macro efi2_category(score_expression) %}
{# Shared eFI2 frailty banding for current and historical scores. #}
case
    when {{ score_expression }} < 0.0857 then 'ROBUST'
    when {{ score_expression }} < 0.1624 then 'MILD FRAILTY'
    when {{ score_expression }} <= 0.2391 then 'MODERATE FRAILTY'
    else 'SEVERE FRAILTY'
end
{% endmacro %}
