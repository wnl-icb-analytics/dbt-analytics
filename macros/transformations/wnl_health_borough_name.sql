{#
    Health borough is the legacy CCG area used for WNL primary care reporting,
    not a London local authority. The org manager app assigns it; rows it has
    not yet assigned default from the registered local authority.
#}
{% macro wnl_health_borough_name(health_borough_column, registered_borough_column) %}
    coalesce(
        {{ health_borough_column }},
        case {{ registered_borough_column }}
            when 'Kensington and Chelsea' then 'West London'
            when 'Westminster' then 'Central London'
            when 'Hammersmith and Fulham' then 'Hammersmith & Fulham'
            else {{ registered_borough_column }}
        end
    )
{% endmacro %}
