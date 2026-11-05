{% test navigation_time_consistency(model, record_id, date_column, at_column, precision_column, at_is_sorting_anchor=false) %}
    select {{ record_id }}
    from {{ model }}
    where {{ precision_column }} not in ('timestamp', 'date', 'month', 'year', 'unknown')
        or {{ precision_column }} is null
        or ({{ date_column }} is null and {{ precision_column }} <> 'unknown')
        or ({{ precision_column }} = 'timestamp' and (
            {{ at_column }} is null or {{ at_column }}::date <> {{ date_column }}
        ))
        {% if at_is_sorting_anchor %}
        or ({{ precision_column }} <> 'timestamp' and {{ at_column }} is distinct from (
            case
                when {{ precision_column }} = 'month' then date_trunc('month', {{ date_column }})::timestamp_ntz
                when {{ precision_column }} = 'year' then date_trunc('year', {{ date_column }})::timestamp_ntz
                else {{ date_column }}::timestamp_ntz
            end
        ))
        {% else %}
        or ({{ precision_column }} <> 'timestamp' and {{ at_column }} is not null)
        {% endif %}
{% endtest %}
