{% macro ltc_lcs_activity_last_valid_date(event_date, inclusive=false) %}
{# Reverse the annual window, including February date clipping in leap years. #}
{% set anniversary = "dateadd(month, 12, " ~ event_date ~ ")" %}
{% if inclusive %}
case when {{ event_date }} >= dateadd(month, -12, dateadd(day, 1, {{ anniversary }}))
    then dateadd(day, 1, {{ anniversary }}) else {{ anniversary }} end
{% else %}
case when {{ event_date }} > dateadd(month, -12, {{ anniversary }})
    then {{ anniversary }} else dateadd(day, -1, {{ anniversary }}) end
{% endif %}
{% endmacro %}
