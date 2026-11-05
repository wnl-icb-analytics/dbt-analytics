{# UTC timestamp_ntz -> Europe/London timestamp_tz. Keeps the offset, so values
   still order correctly through the repeated autumn hour, while DATE() gives
   the London date. #}
{% macro utc_to_london(column_name) %}
    convert_timezone('Europe/London', to_timestamp_tz(date_part(epoch_nanosecond, {{ column_name }}), 9))
{% endmacro %}
