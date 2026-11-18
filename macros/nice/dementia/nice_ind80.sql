{#-
    Calculate NICE IND80 for its eligible population at each reference date.
    Args: reference is current or by_month.
    Returns: the IND80 detail columns, one person per reporting_date.
-#}
{% macro nice_ind80(reference='current') %}
-- NICE IND80: https://www.nice.org.uk/indicators/ind80
-- Eight baseline blood tests within six months either side of a first dementia diagnosis.
-- The rolling annual diagnosis cohort ends six months before the reporting date.
SELECT
    population.person_id,
    'IND80' AS indicator_id,
    'Dementia: target organ damage (new diagnoses)' AS indicator_name,
    'The percentage of patients with a new diagnosis of dementia recorded in the preceding 1 April to 31 March with a record of FBC, calcium, glucose, renal and liver function, thyroid function tests, serum vitamin B12 and folate levels recorded between 6 months before or after entering on to the register.' AS indicator_description,
    profile.reporting_date,
    DATEADD(month, -18, profile.reporting_date) AS measurement_period_start,
    population.age,
    'Dementia diagnosed 18 to 6 months ago' AS denominator_description,
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
WHERE profile.diagnosis_date > DATEADD(month, -18, profile.reporting_date)
    AND profile.diagnosis_date <= DATEADD(month, -6, profile.reporting_date)
{% endmacro %}
