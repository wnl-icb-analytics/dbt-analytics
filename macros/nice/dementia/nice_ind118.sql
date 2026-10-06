{#-
    Calculate NICE IND118 for its eligible population at each reference date.
    Args: reference is current or by_month.
    Returns: the IND118 detail columns, one person per reporting_date.
-#}
{% macro nice_ind118(reference='current') %}
-- NICE IND118: https://www.nice.org.uk/indicators/ind118
-- Eight baseline blood tests in the twelve months ending at first dementia diagnosis, for diagnoses since 1 April 2014.
SELECT
    population.person_id,
    'IND118' AS indicator_id,
    'Dementia: target organ damage (all patients)' AS indicator_name,
    'History Download indicator (PDF) Overview Indicator On this page' AS indicator_description,
    profile.reporting_date,
    '2014-04-01'::DATE AS measurement_period_start,
    population.age,
    'Dementia diagnosed since 1 April 2014' AS denominator_description,
    {{ nice_practice_columns('population', reference) }},
    profile.diagnosis_date,
    profile.ind118_fbc_date AS fbc_date,
    profile.ind118_calcium_date AS calcium_date,
    profile.ind118_glucose_date AS glucose_date,
    profile.ind118_renal_date AS renal_date,
    profile.ind118_liver_date AS liver_date,
    profile.ind118_thyroid_date AS thyroid_date,
    profile.ind118_b12_date AS b12_date,
    profile.ind118_folate_date AS folate_date,
    profile.ind118_latest_record_date AS latest_record_date,
    TRUE AS is_in_denominator,
    profile.has_ind118_all_tests AS is_in_numerator,
    IFF(profile.has_ind118_all_tests, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM {{ nice_ref('int_nice_dementia_baseline_tests', reference) }} AS profile
INNER JOIN ({{ nice_reference_population(reference) }}) AS population
    ON population.person_id = profile.person_id
    AND population.reporting_date = profile.reporting_date
WHERE profile.diagnosis_date >= '2014-04-01'::DATE
{% endmacro %}
