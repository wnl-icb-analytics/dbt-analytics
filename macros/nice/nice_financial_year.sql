{% macro nice_financial_year_start(date_expr) %}
{#-
    Supply the inclusive 1 April start of the financial year containing a date.
    Args: date_expr is a SQL date expression.
    Returns: a DATE expression, without a column alias.
-#}
DATE_FROM_PARTS(YEAR({{ date_expr }}) - IFF(MONTH({{ date_expr }}) < 4, 1, 0), 4, 1)
{% endmacro %}

{% macro nice_financial_year_end(date_expr) %}
{#-
    Supply the inclusive 31 March end of the financial year containing a date.
    Args: date_expr is a SQL date expression.
    Returns: a DATE expression, without a column alias.
-#}
DATEADD(day, -1, DATEADD(year, 1, {{ nice_financial_year_start(date_expr) }}))
{% endmacro %}
