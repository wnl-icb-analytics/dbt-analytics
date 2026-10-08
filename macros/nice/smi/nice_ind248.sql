{% macro nice_ind248(reference='current') %}
{#-
    Calculate NICE IND248 using paired SMI population and evidence.
    Args: reference is current or by_month.
    Returns: one eligible person per reporting_date, with the existing detail columns.
-#}
-- A valid earlier BMI remains evidence even when a later BMI is invalid.
-- NICE IND248: https://www.nice.org.uk/indicators/ind248
-- All six physical health checks in 12 months for people with an active SMI diagnosis.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.birth_date_approx,
        population.practice_code,
        population.practice_name,
        smoking.latest_smoking_status_date,
        physical.latest_blood_pressure_date,
        physical.latest_bmi_date,
        physical.latest_alcohol_record_date,
        physical.latest_lipid_date,
        physical.latest_glucose_or_hba1c_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_smoking_evidence', reference) }} AS smoking
        ON population.person_id = smoking.person_id
        AND population.reporting_date = smoking.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_physical_health_evidence', reference) }} AS physical
        ON population.person_id = physical.person_id
        AND population.reporting_date = physical.reporting_date
    WHERE profile.has_active_smi_diagnosis
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.latest_blood_pressure_date AS latest_blood_pressure_date,
        population.latest_bmi_date AS latest_bmi_date,
        population.latest_alcohol_record_date AS latest_alcohol_record_date,
        population.latest_lipid_date AS latest_lipid_date,
        population.latest_glucose_or_hba1c_date AS latest_glucose_or_hba1c_date,
        population.latest_smoking_status_date AS latest_smoking_status_date,
        IFF(population.latest_blood_pressure_date >= DATEADD(month, -12, population.reporting_date), 1, 0)
            + IFF(population.latest_bmi_date >= DATEADD(month, -12, population.reporting_date), 1, 0)
            + IFF(population.latest_alcohol_record_date >= DATEADD(month, -12, population.reporting_date), 1, 0)
            + IFF(population.latest_lipid_date >= DATEADD(month, -12, population.reporting_date), 1, 0)
            + IFF(population.latest_glucose_or_hba1c_date >= DATEADD(month, -12, population.reporting_date), 1, 0)
            + IFF(population.latest_smoking_status_date >= DATEADD(month, -12, population.reporting_date), 1, 0) AS checks_met_count,
        CASE WHEN population.latest_blood_pressure_date >= DATEADD(month, -12, population.reporting_date)
            AND population.latest_bmi_date >= DATEADD(month, -12, population.reporting_date)
            AND population.latest_alcohol_record_date >= DATEADD(month, -12, population.reporting_date)
            AND population.latest_lipid_date >= DATEADD(month, -12, population.reporting_date)
            AND population.latest_glucose_or_hba1c_date >= DATEADD(month, -12, population.reporting_date)
            AND population.latest_smoking_status_date >= DATEADD(month, -12, population.reporting_date)
            THEN GREATEST(
                population.latest_blood_pressure_date,
                population.latest_bmi_date,
                population.latest_alcohol_record_date,
                population.latest_lipid_date,
                population.latest_glucose_or_hba1c_date,
                population.latest_smoking_status_date
            ) END AS latest_record_date,
        COALESCE(population.latest_blood_pressure_date >= DATEADD(month, -12, population.reporting_date), FALSE)
            AND COALESCE(population.latest_bmi_date >= DATEADD(month, -12, population.reporting_date), FALSE)
            AND COALESCE(population.latest_alcohol_record_date >= DATEADD(month, -12, population.reporting_date), FALSE)
            AND COALESCE(population.latest_lipid_date >= DATEADD(month, -12, population.reporting_date), FALSE)
            AND COALESCE(population.latest_glucose_or_hba1c_date >= DATEADD(month, -12, population.reporting_date), FALSE)
            AND COALESCE(population.latest_smoking_status_date >= DATEADD(month, -12, population.reporting_date), FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND248' AS indicator_id,
    'Bipolar, schizophrenia and other psychoses: 6 physical health checks' AS indicator_name,
    'Percentage of patients with schizophrenia, bipolar affective disorder and other psychoses who, in the preceding 12 months, received all 6 elements of physical health checks for people with severe mental illness.' AS indicator_description,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Active severe mental illness' AS denominator_description,
    {{ nice_practice_columns('assessed', reference) }},
    latest_blood_pressure_date,
    latest_bmi_date,
    latest_alcohol_record_date,
    latest_lipid_date,
    latest_glucose_or_hba1c_date,
    latest_smoking_status_date,
    checks_met_count,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
