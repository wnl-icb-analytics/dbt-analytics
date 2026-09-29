{% macro nice_flu_season_year() -%}
{#- Year in which the most recently completed NICE flu season began. The season runs
    1 August to 31 March, so from 1 April the season that began the previous August
    is complete; before that, the one that began two Augusts ago. -#}
IFF(MONTH(CURRENT_DATE()) >= 4, YEAR(CURRENT_DATE()) - 1, YEAR(CURRENT_DATE()) - 2)
{%- endmacro %}
