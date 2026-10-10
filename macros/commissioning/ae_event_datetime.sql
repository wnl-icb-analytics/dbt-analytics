{#
    Rebuilds a full timestamp for an A&E event that is stored as a bare
    time-of-day, resolving the midnight rollover.

    SUS records arrival as a date + time, but every subsequent event
    (departure, treatment, conclusion...) carries only a time. If that time
    falls earlier in the day than the arrival time, the event must have
    happened after midnight, so it belongs to the following date.

        arrival 22:40, departure 23:15  ->  same date
        arrival 22:40, departure 01:15  ->  next date

    Mirrors the DATEDIFF(SECOND, ...) < 0 THEN DATEADD(d,1,...) pattern
    repeated seven times in ETL.postProcess_AE_Current.

    Returns NULL when either time is NULL, matching T-SQL behaviour.

    Usage:
      {{ ae_event_datetime('arrival_date', 'arrival_time', 'em_departure_time') }}
        as z_em_departure_datetime
#}

{% macro ae_event_datetime(arrival_date, arrival_time, event_time) %}
    case
        when {{ event_time }} is null or {{ arrival_time }} is null then null
        else timestamp_ntz_from_parts(
            case
                when to_time({{ event_time }}) < to_time({{ arrival_time }})
                    then dateadd(day, 1, to_date({{ arrival_date }}))
                else to_date({{ arrival_date }})
            end,
            to_time({{ event_time }})
        )
    end
{% endmacro %}
