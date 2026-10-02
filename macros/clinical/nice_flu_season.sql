{% macro nice_flu_season_year(date_expr='CURRENT_DATE()') -%}
{#- Year in which the most recently completed NICE flu season began. The season runs
    1 August to 31 March, so from 1 April the season that began the previous August
    is complete; before that, the one that began two Augusts ago.
    Args: date_expr is the SQL reference-date expression; omitted means today.
    Returns: the integer year in which the selected completed season began. -#}
IFF(MONTH({{ date_expr }}) >= 4, YEAR({{ date_expr }}) - 1, YEAR({{ date_expr }}) - 2)
{%- endmacro %}
