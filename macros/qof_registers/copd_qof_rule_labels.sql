{% macro copd_qof_rule_labels() %}
{#- Labels for the QOF v51 COPD register rules, keyed by rule number. -#}
{{ return({
    1: 'Rule 1: Pre-April 2023',
    2: 'Rule 2: Post-April 2023 + Spirometry',
    3: 'Rule 3: Newly Registered + Spirometry',
    4: 'Rule 4: Post-April 2023 (All Remaining)'
}) }}
{% endmacro %}
