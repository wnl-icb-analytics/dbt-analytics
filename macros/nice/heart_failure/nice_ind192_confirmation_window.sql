{% macro nice_ind192_confirmation_window(evidence_date, diagnosis_date, reporting_date) -%}
{{ evidence_date }} BETWEEN DATEADD(month, -3, {{ diagnosis_date }})
    AND LEAST(DATEADD(month, 3, {{ diagnosis_date }}), {{ reporting_date }})
{%- endmacro %}
