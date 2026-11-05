/*
ICB and Sub-ICB macros to pull reference codes to allow filtering on ICB or 
Sub-ICB level geographies without hardcoding values or performing a join.
*/

--Values here are defined primarily in the dbt_project.yml file. 
--Default values are included below if the variable is unavailable in the yml.

{% macro wnl_ods_codes() %}
    {{ to_sql_list(var('wnl_ods_codes', ['Z9B2Z', 'QMJ', 'QRV'])) }}
{% endmacro %}

{% macro ncl_sub_icb() %}
    '{{ var("ncl_sub_icb", "93C") }}'
{% endmacro %}

{% macro ncl_sub_icb_incl_ccg() %}
    {{ to_sql_list(var("ncl_sub_icb_incl_ccg", ['93C', '07M', '07R', '07X', '08D', '08H'])) }}
{% endmacro %}

{% macro ncl_trust_codes_acute() %}
    {{ to_sql_list(var('ncl_trust_codes_acute', ['RP4', 'RP6', 'RAL', 'RAN', 'RRV', 'RKE', 'RAP'])) }}
{% endmacro %}

{% macro ncl_trust_codes_full() %}
    {{ to_sql_list(var('ncl_trust_codes_full', ['RP4', 'RP6', 'RAL', 'RAN', 'RRV', 'RKE', 'G6V2S', 'RNK', 'RAP', 'RRP', 'TAF'])) }}
{% endmacro %}

{% macro ncl_borough_names() %}
    {{ to_sql_list(var('ncl_borough_names', ['Barnet', 'Camden', 'Enfield', 'Islington', 'Haringey'])) }}
{% endmacro %}

{% macro ncl_borough_codes() %}
    {{ to_sql_list(var('ncl_borough_codes', ['E09000003', 'E09000007', 'E09000010', 'E09000014', 'E09000019'])) }}
{% endmacro %}

{% macro ncl_sk_parent_codes() %}
    {{ to_sql_list(var('ncl_sk_parent_codes', ['427608', '305562', '305574', '305584', '305602', '305607'])) }}
{% endmacro %}

{% macro nwl_sub_icb() %}
    '{{ var("nwl_sub_icb", "W2U3Z") }}'
{% endmacro %}

{% macro nwl_sub_icb_incl_ccg() %}
    {{ to_sql_list(var("nwl_sub_icb_incl_ccg", ['W2U3Z', '07P', '07W', '08C', '08E', '08G', '07Y', '08Y', '09A']))}}
{% endmacro %}

{% macro nwl_trust_codes_acute() %}
    {{ to_sql_list(var('nwl_trust_codes_acute', ['RAS', 'RYJ', 'RQM', 'R1K'])) }}
{% endmacro %}

{% macro nwl_trust_codes_full() %}
    {{ to_sql_list(var('nwl_trust_codes_full', ['RAS', 'RYJ', 'RQM', 'R1K', 'RKL', 'RV3', 'RYX'])) }}
{% endmacro %}

{% macro nwl_borough_names() %}
    {{ to_sql_list(var('nwl_borough_names', ['Brent', 'Ealing', 'Hammersmith and Fulham', 'Harrow', 'Hillingdon', 'Hounslow', 'Kensington and Chelsea', 'Westminster', 'West London'])) }}
{% endmacro %}

{% macro nwl_borough_codes() %}
    {{ to_sql_list(var('nwl_borough_codes', ['E09000005', 'E09000009', 'E09000013', 'E09000015', 'E09000017', 'E09000018', 'E09000020', 'E09000033'])) }}
{% endmacro %}