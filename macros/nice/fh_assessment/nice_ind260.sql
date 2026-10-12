{% macro nice_ind260(reference='current') %}
{#-
    Calculate NICE IND260 for eligible high-cholesterol patients at each reference date.
    Args: reference is current or by_month.
    Returns: the IND260 detail columns, one person per reporting_date.
-#}
-- NICE IND260: https://www.nice.org.uk/indicators/ind260
WITH assessed AS (
    SELECT profile.person_id, profile.reporting_date, population.age,
        population.practice_code, population.practice_name,
        profile.earliest_qualifying_reading_date AS measurement_period_start,
        profile.earliest_qualifying_reading_date AS qualifying_reading_date,
        profile.earliest_qualifying_cholesterol_value AS qualifying_cholesterol_value,
        profile.age_at_earliest_qualifying_reading,
        profile.first_fh_assessment_date,
        profile.first_clinical_fh_diagnosis_date,
        profile.first_fh_referral_date,
        profile.first_genetic_fh_date,
        profile.latest_secondary_hyperlipidaemia_date,
        (COALESCE(profile.latest_secondary_hyperlipidaemia_date > profile.earliest_qualifying_reading_date, FALSE)) AS has_qualifying_secondary_hyperlipidaemia,
        profile.first_fh_assessment_date IS NOT NULL
            OR profile.first_clinical_fh_diagnosis_date IS NOT NULL
            OR profile.first_fh_referral_date IS NOT NULL
            OR profile.first_genetic_fh_date IS NOT NULL AS has_fh_evidence
    FROM {{ nice_ref('int_nice_fh_assessment', reference) }} AS profile
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON profile.person_id = population.person_id AND profile.reporting_date = population.reporting_date
    WHERE profile.earliest_qualifying_reading_date IS NOT NULL
)
SELECT
    person_id,
    'IND260' AS indicator_id,
    'Lipid disorders: FH assessment and diagnosis (historical readings)' AS indicator_name,
    'The percentage of patients with a total cholesterol reading greater than 7.5 mmol/litre when aged 29 years or under, or greater than 9.0 mmol/litre when aged 30 years or over, who have been: diagnosed with secondary hyperlipidaemia or clinically assessed for familial hypercholesterolaemia or referred for assessment for familial hypercholesterolaemia or genetically diagnosed with familial hypercholesterolaemia.' AS indicator_description,
    reporting_date,
    measurement_period_start,
    age,
    'Historical high cholesterol' AS denominator_description,
    {{ nice_practice_columns('result', reference) }},
    qualifying_reading_date,
    qualifying_cholesterol_value,
    age_at_earliest_qualifying_reading,
    first_fh_assessment_date,
    first_clinical_fh_diagnosis_date,
    first_fh_referral_date,
    first_genetic_fh_date,
    latest_secondary_hyperlipidaemia_date,
    has_qualifying_secondary_hyperlipidaemia,
    GREATEST_IGNORE_NULLS(first_fh_assessment_date, first_clinical_fh_diagnosis_date, first_fh_referral_date, first_genetic_fh_date,
        IFF(has_qualifying_secondary_hyperlipidaemia, latest_secondary_hyperlipidaemia_date, NULL))::DATE AS latest_record_date,
    TRUE AS is_in_denominator,
    has_fh_evidence OR has_qualifying_secondary_hyperlipidaemia AS is_in_numerator,
    IFF(is_in_numerator, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed AS result
{% endmacro %}
