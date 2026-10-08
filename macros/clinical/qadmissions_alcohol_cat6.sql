{% macro qadmissions_alcohol_cat6(score_expression) %}
{# Shared QAdmissions alcohol_cat6 banding of a Full AUDIT score for the
   current and historical feature models. Callers restrict to valid 0-40
   scores. The bands are continuous, with the same upper bounds as the risk
   categories in int_alcohol_audit_scores, so a fractional score falls in
   the band whose upper bound it reaches (3.5 -> 2, 19.5 -> 5). #}
case
    when {{ score_expression }} = 0   then 0
    when {{ score_expression }} <= 3  then 1
    when {{ score_expression }} <= 7  then 2
    when {{ score_expression }} <= 15 then 3
    when {{ score_expression }} <= 19 then 4
    when {{ score_expression }} > 19  then 5
end
{% endmacro %}
