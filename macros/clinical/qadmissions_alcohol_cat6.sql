{% macro qadmissions_alcohol_cat6(score_expression) %}
{# Shared QAdmissions alcohol_cat6 banding of a Full AUDIT score for the
   current and historical feature models. Callers restrict to valid 0-40
   scores. A fractional score that falls between bands returns NULL. #}
case
    when {{ score_expression }} = 0                 then 0
    when {{ score_expression }} between  1 and  3   then 1
    when {{ score_expression }} between  4 and  7   then 2
    when {{ score_expression }} between  8 and 15   then 3
    when {{ score_expression }} between 16 and 19   then 4
    when {{ score_expression }} >= 20               then 5
end
{% endmacro %}
