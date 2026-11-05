{% macro observation_clinical_date(alias='o') %}
    {#- Return NULL for a missing clinical date; callers choose their missing-date rule.
        A clinical date after a real recorded date becomes the recorded date.
        Pre-1900 recorded dates are EMIS migration placeholders, so keep the clinical date.
        Mixing DATE and TIMESTAMP_NTZ preserves a TIMESTAMP_NTZ result. -#}
    CASE
        WHEN {{ alias }}.clinical_effective_date > {{ alias }}.date_recorded
            AND {{ alias }}.date_recorded >= '1900-01-01'
            THEN {{ alias }}.date_recorded
        ELSE {{ alias }}.clinical_effective_date
    END
{%- endmacro %}
