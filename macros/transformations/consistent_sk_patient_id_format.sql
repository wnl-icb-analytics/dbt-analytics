{# Coerce a patient surrogate-key expression to VARCHAR. Use for every sk_patient_id
   column in commissioning staging models so a future switch to numeric keys can
   be made in one place by updating this macro. Note, if considering switch to numeric then
   consider the colliding IDs with leading 0 e.g., 070 and 70 would colide. This was not observed
   when checked on 2026/09/04. A test has been added to flag if keys that aren't numeric begin to 
   flow as the hxflake_pseudo_generation using a numeric transformation of this key. 1 used as a null
   catch all so removing. Reintroduce if one has meaning #}
{% macro consistent_sk_patient_id_format(column) -%}
    {%- if column is not string or column | trim == '' -%}
        {{ exceptions.raise_compiler_error(
            "consistent_sk_patient_id_format() expects a quoted column name or "
            ~ "SQL expression, but received an undefined or empty value. Check "
            ~ "for a missing quote, e.g. (patient_id) instead of ('patient_id')."
        ) }}
    {%- endif -%}
    nullif(nullif(trim(cast({{ column }} as varchar)), ''), '1')
{%- endmacro %}