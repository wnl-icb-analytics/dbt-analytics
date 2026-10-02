{% macro nice_financial_year_start(date_expr) %}
DATE_FROM_PARTS(YEAR({{ date_expr }}) - IFF(MONTH({{ date_expr }}) < 4, 1, 0), 4, 1)
{% endmacro %}

{% macro nice_financial_year_end(date_expr) %}
DATEADD(day, -1, DATEADD(year, 1, {{ nice_financial_year_start(date_expr) }}))
{% endmacro %}
