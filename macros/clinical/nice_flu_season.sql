{% macro nice_flu_season_year(date_expr='CURRENT_DATE()') -%}
{#- Year in which the most recently completed NICE flu season began. The season runs
    1 August to 31 March, inclusive. On 31 March the selected season is the one
    ending that day; earlier dates use the season that began two Augusts ago.
    Args: date_expr is the SQL reference-date expression; omitted means today.
    Returns: the integer year in which the selected completed season began. -#}
IFF({{ date_expr }} >= DATE_FROM_PARTS(YEAR({{ date_expr }}), 3, 31), YEAR({{ date_expr }}) - 1, YEAR({{ date_expr }}) - 2)
{%- endmacro %}
