{% macro nice_ind233(reference='current') %}
{#-
    Calculate NICE IND233 for eligible CKD members at each reference date.
    Args: reference is current or by_month.
    Returns: the IND233 detail columns, one person per reporting_date.
-#}
-- NICE IND233: https://www.nice.org.uk/indicators/ind233
-- eGFR on two occasions at least 90 days apart, the second within 90 days before diagnosis, for people diagnosed with CKD stage 3 to 5 in the preceding 12 months.
WITH indicator_population AS (
    SELECT
        profile.person_id,
        profile.reporting_date,
        profile.ckd_diagnosis_date,
        profile.has_egfr_pair_before_diagnosis,
        profile.latest_acr_value,
        profile.latest_egfr_value,
        profile.second_egfr_before_diagnosis_date,
        population.age,
        population.practice_code,
        population.practice_name
    FROM {{ nice_ref('int_ckd_profile', reference) }} AS profile
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON profile.person_id = population.person_id
        AND profile.reporting_date = population.reporting_date
    WHERE profile.ckd_diagnosis_date
        BETWEEN DATEADD(month, -12, population.reporting_date) AND population.reporting_date
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.ckd_diagnosis_date AS diagnosis_date,
        population.latest_egfr_value,
        population.latest_acr_value,
        CASE
            WHEN population.has_egfr_pair_before_diagnosis
                THEN population.second_egfr_before_diagnosis_date
        END AS latest_record_date,
        population.has_egfr_pair_before_diagnosis AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND233' AS indicator_id,
    'Kidney conditions: CKD and eGFR' AS indicator_name,
    'The percentage of patients with a new diagnosis of CKD stage G3a–G5 (on the register, within the preceding 12 months) who had eGFR measured on at least 2 occasions separated by at least 90 days, and the second test within 90 days before the diagnosis.' AS indicator_description,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'New chronic kidney disease, stages 3 to 5' AS denominator_description,
    {{ nice_practice_columns('result', reference) }},
    diagnosis_date,
    latest_egfr_value,
    latest_acr_value,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed AS result

{% endmacro %}
