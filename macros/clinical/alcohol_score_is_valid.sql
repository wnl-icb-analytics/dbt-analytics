{% macro alcohol_score_is_valid(value, instrument) -%}
    COALESCE(
        {{ value }} = FLOOR({{ value }})
        AND (({{ instrument }} = 'FAST' AND {{ value }} BETWEEN 0 AND 16)
          OR ({{ instrument }} = 'AUDIT-C' AND {{ value }} BETWEEN 0 AND 12)
          OR ({{ instrument }} = 'AUDIT-PC' AND {{ value }} BETWEEN 0 AND 20)
          OR ({{ instrument }} = 'AUDIT' AND {{ value }} BETWEEN 0 AND 40)),
        FALSE
    )
{%- endmacro %}
