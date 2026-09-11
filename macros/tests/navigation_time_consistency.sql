{% test navigation_time_consistency(model, record_id, date_column, at_column, precision_column) %}
    select {{ record_id }}
    from {{ model }}
    where {{ precision_column }} not in ('timestamp', 'date', 'month', 'year', 'unknown')
        or {{ precision_column }} is null
        or ({{ date_column }} is null and {{ precision_column }} <> 'unknown')
        or ({{ precision_column }} = 'timestamp' and (
            {{ at_column }} is null or {{ at_column }}::date <> {{ date_column }}
        ))
        or ({{ precision_column }} <> 'timestamp' and {{ at_column }} is not null)
{% endtest %}
