{% macro calculate_acp_from_activity_date(activity_date) %}
    'M' || to_varchar(month(dateadd('month', -3, {{ activity_date }})))
        || ' - ' || to_varchar({{ activity_date }}, 'Mon')
        || ' ' || to_varchar(year({{ activity_date }}))
{% endmacro %}