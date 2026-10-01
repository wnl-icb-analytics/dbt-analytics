{#
    Parse a TAT datetime string to TIMESTAMP_NTZ.
    Providers send UK order 'DD/MM/YYYY HH:MI' (no seconds); xlsx-converted files
    arrive ISO via pandas. COALESCE covers both, with/without seconds, and date-only.
#}
{% macro tat_parse_ts(column_name) %}
    coalesce(
        try_to_timestamp_ntz({{ column_name }}, 'DD/MM/YYYY HH24:MI'),
        try_to_timestamp_ntz({{ column_name }}, 'DD/MM/YYYY HH24:MI:SS'),
        try_to_timestamp_ntz({{ column_name }}, 'DD/MM/YYYY'),
        try_to_timestamp_ntz({{ column_name }})
    )
{% endmacro %}
