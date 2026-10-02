{% macro nice_ind118(reference='current') %}
SELECT
    population.person_id,
    'IND118' AS indicator_id,
    'Dementia: baseline tests' AS indicator_name,
    profile.reporting_date,
    '2014-04-01'::DATE AS measurement_period_start,
    population.age,
    'Dementia' AS condition_name,
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
