{% macro bp_control_context_order(achieves_expression) %}
{# Boolean SQL expression; requires is_valid_bp, bp_context_group, date_recorded
   and observation_id. Prefer achievement, validity, clinic, home, then ABPM;
   remaining ties use the latest save time and highest observation id. #}
({{ achieves_expression }}) DESC NULLS LAST,
is_valid_bp DESC NULLS LAST,
CASE bp_context_group WHEN 'CLINIC' THEN 1 WHEN 'HOME' THEN 2 ELSE 3 END,
date_recorded DESC NULLS LAST, observation_id DESC
{% endmacro %}
