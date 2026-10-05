{% macro nice_ind234(reference='current') %}
{#-
    Calculate NICE IND234 for eligible CKD members at each reference date.
    Args: reference is current or by_month.
    Returns: the IND234 detail columns, one person per reporting_date.
-#}
-- NICE IND234: https://www.nice.org.uk/indicators/ind234
-- eGFR and urine ACR both recorded within 90 days before or after diagnosis for people diagnosed with CKD stage 3 to 5 in the preceding 12 months.
WITH indicator_population AS (
    SELECT
        profile.person_id,
        profile.reporting_date,
        profile.acr_within_90_days_of_diagnosis_date,
        profile.ckd_diagnosis_date,
        profile.egfr_within_90_days_of_diagnosis_date,
        profile.has_acr_within_90_days_of_diagnosis,
        profile.has_egfr_within_90_days_of_diagnosis,
        profile.latest_acr_value,
        profile.latest_egfr_value,
        population.age,
        population.practice_code,
        population.practice_name
    FROM {{ nice_ref('int_ckd_profile', reference) }} AS profile
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON profile.person_id = population.person_id
        AND profile.reporting_date = population.reporting_date
    WHERE profile.ckd_diagnosis_date > DATEADD(month, -12, DATEADD(day, -90, population.reporting_date)) AND profile.ckd_diagnosis_date <= DATEADD(day, -90, population.reporting_date)
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
            WHEN population.has_egfr_within_90_days_of_diagnosis AND population.has_acr_within_90_days_of_diagnosis
                THEN GREATEST(population.egfr_within_90_days_of_diagnosis_date, population.acr_within_90_days_of_diagnosis_date)
        END AS latest_record_date,
        population.has_egfr_within_90_days_of_diagnosis AND population.has_acr_within_90_days_of_diagnosis AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND234' AS indicator_id,
    'Kidney conditions: CKD – eGFR and ACR' AS indicator_name,
    reporting_date,
    DATEADD(month, -12, DATEADD(day, -90, reporting_date)) AS measurement_period_start,
    age,
    'New chronic kidney disease, stages 3 to 5'::VARCHAR(53) AS condition_name,
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
