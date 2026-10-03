{% macro nice_fh_assessment_union(reference='current') %}
{#- Combine familial hypercholesterolaemia assessment measures at person, indicator and date grain. -#}
SELECT
    person_id,
    indicator_id,
    indicator_name,
    reporting_date,
    measurement_period_start,
    age,
    condition_name,
    {{ nice_practice_columns(none, reference) }},
    qualifying_reading_date,
    qualifying_cholesterol_value,
    age_at_earliest_qualifying_reading,
    first_fh_assessment_date,
    first_clinical_fh_diagnosis_date,
    first_fh_referral_date,
    first_genetic_fh_date,
    latest_secondary_hyperlipidaemia_date,
    NULL::DATE AS first_secondary_history_date,
    has_qualifying_secondary_hyperlipidaemia,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_fh_assessment_historical_cholesterol_ind260' if reference == 'current' else 'fct_person_fh_assessment_historical_cholesterol_ind260_by_month') }}

UNION ALL

SELECT
    person_id,
    indicator_id,
    indicator_name,
    reporting_date,
    measurement_period_start,
    age,
    condition_name,
    {{ nice_practice_columns(none, reference) }},
    qualifying_reading_date,
    qualifying_cholesterol_value,
    NULL::NUMBER AS age_at_earliest_qualifying_reading,
    first_fh_assessment_date,
    first_clinical_fh_diagnosis_date,
    first_fh_referral_date,
    first_genetic_fh_date,
    latest_secondary_hyperlipidaemia_date,
    first_secondary_history_date,
    has_qualifying_secondary_hyperlipidaemia,
    latest_record_date,
    is_in_denominator,
    is_in_numerator,
    indicator_status
FROM {{ ref('fct_person_fh_assessment_recent_cholesterol_ind261' if reference == 'current' else 'fct_person_fh_assessment_recent_cholesterol_ind261_by_month') }}
{% endmacro %}
