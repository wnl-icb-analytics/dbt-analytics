{% macro alcohol_score_is_valid(value, instrument) -%}
{#-
    Check a whole-number alcohol screening total.
    Args: value is a numeric SQL expression; instrument is a SQL expression
          returning FAST, AUDIT-C, AUDIT-PC or AUDIT.
    Returns: TRUE within the inclusive ranges FAST 0-16, AUDIT-C 0-12,
             AUDIT-PC 0-20 or AUDIT 0-40. Null or unknown inputs return FALSE.
-#}
    COALESCE(
        {{ value }} = FLOOR({{ value }})
        AND (({{ instrument }} = 'FAST' AND {{ value }} BETWEEN 0 AND 16)
          OR ({{ instrument }} = 'AUDIT-C' AND {{ value }} BETWEEN 0 AND 12)
          OR ({{ instrument }} = 'AUDIT-PC' AND {{ value }} BETWEEN 0 AND 20)
          OR ({{ instrument }} = 'AUDIT' AND {{ value }} BETWEEN 0 AND 40)),
        FALSE
    )
{%- endmacro %}
