{% macro nice_ind80(reference='current') %}
SELECT
    population.person_id,
    'IND80' AS indicator_id,
    'Dementia: baseline tests' AS indicator_name,
    profile.reporting_date,
    {{ nice_financial_year_start('profile.reporting_date') }} AS measurement_period_start,
    population.age,
    'Dementia' AS condition_name,
    {{ nice_practice_columns('population', reference) }},
    profile.diagnosis_date,
    profile.ind80_fbc_date AS fbc_date,
    profile.ind80_calcium_date AS calcium_date,
    profile.ind80_glucose_date AS glucose_date,
    profile.ind80_renal_date AS renal_date,
    profile.ind80_liver_date AS liver_date,
    profile.ind80_thyroid_date AS thyroid_date,
    profile.ind80_b12_date AS b12_date,
    profile.ind80_folate_date AS folate_date,
    profile.ind80_latest_record_date AS latest_record_date,
    TRUE AS is_in_denominator,
    profile.has_ind80_all_tests AS is_in_numerator,
    IFF(profile.has_ind80_all_tests, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM {{ nice_ref('int_nice_dementia_baseline_tests', reference) }} AS profile
INNER JOIN ({{ nice_reference_population(reference) }}) AS population
    ON population.person_id = profile.person_id
    AND population.reporting_date = profile.reporting_date
WHERE profile.diagnosis_date BETWEEN {{ nice_financial_year_start('profile.reporting_date') }} AND profile.reporting_date
{% endmacro %}
