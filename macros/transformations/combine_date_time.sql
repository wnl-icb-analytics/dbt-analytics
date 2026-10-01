{# Combine a parsed DATE and a provider-typed time string into a TIMESTAMP_NTZ.
   The time is stripped to digits/colon (drops fractional seconds and stray
   text) and matched as HH:MM:SS then HH:MM; if the time is absent or
   unparseable the date is returned at midnight. NULL date -> NULL.
   `d` is a DATE expression, `t` a time string expression. #}
{% macro combine_date_time(d, t) %}
    case when {{ d }} is not null then
        coalesce(
            try_to_timestamp_ntz(
                to_char({{ d }}, 'YYYY-MM-DD') || ' '
                || left(regexp_replace({{ t }}, '[^0-9:]', ''), 8),
                'YYYY-MM-DD HH24:MI:SS'),
            try_to_timestamp_ntz(
                to_char({{ d }}, 'YYYY-MM-DD') || ' '
                || left(regexp_replace({{ t }}, '[^0-9:]', ''), 5),
                'YYYY-MM-DD HH24:MI'),
            {{ d }}::timestamp_ntz
        )
    end
{% endmacro %}
