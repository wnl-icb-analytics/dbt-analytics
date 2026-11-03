{% macro bp_control_context_order(achieves_expression) %}
({{ achieves_expression }}) DESC NULLS LAST,
is_valid_bp DESC NULLS LAST,
CASE bp_context_group WHEN 'CLINIC' THEN 1 WHEN 'HOME' THEN 2 ELSE 3 END,
date_recorded DESC NULLS LAST, observation_id DESC
{% endmacro %}
