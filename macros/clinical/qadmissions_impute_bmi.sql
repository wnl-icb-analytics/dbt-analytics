{#
QAdmissions BMI imputation

Shared by qadmissions_input_features and qadmissions_input_features_validation.
A missing BMI is filled with the mean recorded BMI of people with the same
sex, age decade and ethnicity group. If that group has fewer than
var('qadmissions_bmi_imputation_min_group', 100) recorded BMIs, the mean for
sex and age decade is used instead, then the mean for sex.

The means use every valid recorded BMI between 10 and 150

Set var('qadmissions_impute_bmi') to false to turn imputation off: bmi is then
the recorded BMI and nothing is flagged as imputed.
#}

{% macro qadmissions_bmi_imputation_method() -%}
{{ 'mean_sex_age_decade_ethnicity_category' if var('qadmissions_impute_bmi', true) else 'none' }}
{%- endmacro %}


{% macro qadmissions_bmi_ethnicity_group(category) -%}
{# The ethnicity group the imputation uses: dim_person_ethnicity's
   ethnicity_category, with 'Unknown' (an ethnicity code with no category) and
   a missing row merged into 'Not Recorded'. #}
CASE
    WHEN {{ category }} IS NULL OR {{ category }} = 'Unknown' THEN 'Not Recorded'
    ELSE {{ category }}
END
{%- endmacro %}


{% macro qadmissions_impute_bmi(source, partition_by=[], min_group=none, enabled=none) %}
{# A SELECT over `source`, a relation with one row per person and the columns
   person_id, sex, age_decade, ethnicity_group and bmi_recorded, plus any
   partition_by columns. The means are computed separately within each
   partition_by value: the history model passes ['end_date'], so each index
   date's means use only that date's population and the BMIs known by then.

   Returns one row per source row: the partition_by columns, person_id,
   bmi_recorded, bmi, is_bmi_imputed and bmi_imputation_group
   ('sex_age_ethnicity', 'sex_age' or 'sex'; NULL when bmi is recorded).

   min_group and enabled default to the qadmissions_bmi_imputation_min_group
   and qadmissions_impute_bmi variables. The models leave them out; the
   synthetic test passes them so it can use small groups and test the switch. #}
{%- set min_group = min_group if min_group is not none else var('qadmissions_bmi_imputation_min_group', 100) -%}
{%- set enabled = enabled if enabled is not none else var('qadmissions_impute_bmi', true) -%}
{%- set levels = [
    ('sex_age_ethnicity', ['sex', 'age_decade', 'ethnicity_group']),
    ('sex_age', ['sex', 'age_decade']),
    ('sex', ['sex']),
] -%}
SELECT
    {%- for column in partition_by %}
    s.{{ column }},
    {%- endfor %}
    s.person_id,
    s.bmi_recorded,
{%- if enabled %}
    -- The finest group with enough recorded BMIs; sex alone is the last resort.
    COALESCE(
        s.bmi_recorded,
        {%- for name, columns in levels %}
        {%- if not loop.last %}
        CASE WHEN m_{{ name }}.n_recorded >= {{ min_group }} THEN m_{{ name }}.mean_bmi END,
        {%- else %}
        m_{{ name }}.mean_bmi
        {%- endif %}
        {%- endfor %}
    )                                                   AS bmi,
    s.bmi_recorded IS NULL                              AS is_bmi_imputed,
    CASE
        WHEN s.bmi_recorded IS NOT NULL THEN NULL
        {%- for name, columns in levels %}
        {%- if not loop.last %}
        WHEN m_{{ name }}.n_recorded >= {{ min_group }} THEN '{{ name }}'
        {%- else %}
        WHEN m_{{ name }}.mean_bmi IS NOT NULL THEN '{{ name }}'
        {%- endif %}
        {%- endfor %}
    END                                                 AS bmi_imputation_group
{%- else %}
    s.bmi_recorded                                      AS bmi,
    FALSE                                               AS is_bmi_imputed,
    CAST(NULL AS VARCHAR)                               AS bmi_imputation_group
{%- endif %}
FROM {{ source }} AS s
{%- if enabled %}
{%- for name, columns in levels %}
{%- set keys = partition_by + columns %}
-- Mean recorded BMI per {{ keys | join(' x ') }}.
LEFT JOIN (
    SELECT
        {{ keys | join(', ') }},
        AVG(bmi_recorded)   AS mean_bmi,
        COUNT(bmi_recorded) AS n_recorded
    FROM {{ source }}
    GROUP BY {{ keys | join(', ') }}
) AS m_{{ name }}
    {%- for key in keys %}
    {{ 'ON ' if loop.first else 'AND' }} m_{{ name }}.{{ key }} = s.{{ key }}
    {%- endfor %}
{%- endfor %}
{%- endif %}
{% endmacro %}
