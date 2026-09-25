{% macro csds_attendance_code(attended_or_did_not_attend_code, attendance_status) -%}
{#- The original attended-or-did-not-attend field first, the newer attendance status otherwise; zero padding removed. -#}
nullif(ltrim(trim(coalesce({{ attended_or_did_not_attend_code }}, {{ attendance_status }})), '0'), '')
{%- endmacro %}
