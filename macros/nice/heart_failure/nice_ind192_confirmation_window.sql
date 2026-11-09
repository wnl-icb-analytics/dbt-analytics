{# Returns an inclusive predicate for three calendar months either side of diagnosis, capped at reporting_date.
   Arguments are SQL date expressions for evidence, diagnosis and reporting. #}
{% macro nice_ind192_confirmation_window(evidence_date, diagnosis_date, reporting_date) -%}
{{ evidence_date }} BETWEEN DATEADD(month, -3, {{ diagnosis_date }})
    AND LEAST(DATEADD(month, 3, {{ diagnosis_date }}), {{ reporting_date }})
{%- endmacro %}
