{% macro csds_attendance_code(attended_or_did_not_attend_code, attendance_status) -%}
{#- The original attended-or-did-not-attend field first, the newer attendance status otherwise; leading zeros removed except for code 0. -#}
{%- set supplied -%}nullif(trim(coalesce({{ attended_or_did_not_attend_code }}, {{ attendance_status }})), ''){%- endset -%}
iff(regexp_like({{ supplied }}, '^0+$'), '0', ltrim({{ supplied }}, '0'))
{%- endmacro %}
