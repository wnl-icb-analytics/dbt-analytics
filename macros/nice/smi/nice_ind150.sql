{% macro nice_ind150(reference='current') %}
{#-
    Calculate NICE IND150 from the paired population and evidence.
    Args: reference is current or by_month.
    Returns: one eligible person per reporting_date, with indicator detail.
-#}
-- NICE IND150: https://www.nice.org.uk/indicators/ind150
-- CVD risk assessment in 12 months for people aged 25 to 84 with an active SMI diagnosis, excluding existing CVD, CKD, familial hypercholesterolaemia and type 1 diabetes.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        evidence.latest_risk_assessment_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_cvd_risk_profile', reference) }} AS evidence
        ON population.person_id = evidence.person_id
        AND population.reporting_date = evidence.reporting_date
    WHERE profile.has_active_smi_diagnosis
        AND population.age BETWEEN 25 AND 84
        AND profile.earliest_cvd_diagnosis_date IS NULL
        AND NOT COALESCE(evidence.has_ckd, FALSE)
        AND NOT COALESCE(evidence.has_familial_hypercholesterolaemia, FALSE)
        AND NOT COALESCE(evidence.has_type1_diabetes, FALSE)
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        CASE
            WHEN population.latest_risk_assessment_date >= DATEADD(month, -12, population.reporting_date)
                THEN population.latest_risk_assessment_date
        END AS latest_record_date,
        COALESCE(population.latest_risk_assessment_date >= DATEADD(month, -12, population.reporting_date), FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND150' AS indicator_id,
    'Cardiovascular disease prevention: cardiovascular risk assessment for people with bipolar, schizophrenia or other psychoses' AS indicator_name,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Severe mental illness (schizophrenia, bipolar affective disorder or other psychoses, not in remission), aged 25 to 84, without CVD, CKD, familial hypercholesterolaemia or type 1 diabetes' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
