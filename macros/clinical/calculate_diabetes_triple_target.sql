{% macro calculate_diabetes_triple_target(reference='current') %}
{#-
    National Diabetes Audit (NDA) treatment targets for diabetes register members.
    Args: reference is current or by_month.
    Returns: one row per register member and reporting_date.
-#}
-- NDA treatment targets: latest HbA1c <= 58 mmol/mol and latest BP <= 140/80 in the
-- audit period, plus a statin prescribed in the period where indicated.
-- Audit period: the 15 calendar months ending in the reporting month (01 Jan to 31 Mar at March).
WITH population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        DATE_TRUNC('month', DATEADD(month, -14, population.reporting_date))::DATE AS measurement_period_start,
        population.age,
        population.practice_code,
        population.practice_name,
        diabetes.diabetes_type,
        -- Statin indication uses GP-coded CVD; the NDA uses hospital admissions for CVD.
        (chd.person_id IS NOT NULL OR stia.person_id IS NOT NULL OR pad.person_id IS NOT NULL) AS has_cvd
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN ({{ nice_register('DM', reference) }}) AS diabetes
        ON population.person_id = diabetes.person_id
        AND population.reporting_date = diabetes.reporting_date
    LEFT JOIN ({{ nice_register('CHD', reference) }}) AS chd
        ON population.person_id = chd.person_id
        AND population.reporting_date = chd.reporting_date
    LEFT JOIN ({{ nice_register('STIA', reference) }}) AS stia
        ON population.person_id = stia.person_id
        AND population.reporting_date = stia.reporting_date
    LEFT JOIN ({{ nice_register('PAD', reference) }}) AS pad
        ON population.person_id = pad.person_id
        AND population.reporting_date = pad.reporting_date
),

assessed AS (
    SELECT
        population.*,
        hba.latest_hba1c_date::DATE AS latest_hba1c_date,
        hba.latest_hba1c_value,
        COALESCE(hba.is_latest_hba1c_valid, FALSE) AS is_latest_hba1c_valid,
        hba.person_id IS NOT NULL AS is_hba1c_recorded_in_period,
        bp.latest_bp_date::DATE AS latest_bp_date,
        bp.latest_systolic_value,
        bp.latest_diastolic_value,
        COALESCE(bp.is_valid_bp, FALSE) AS is_valid_bp,
        bp.person_id IS NOT NULL AS is_bp_recorded_in_period,
        therapy.latest_statin_order_date::DATE AS latest_statin_order_date,
        COALESCE(therapy.latest_statin_order_date::DATE >= population.measurement_period_start, FALSE)
            AS is_statin_prescribed_in_period,
        -- Combined CVD prevention group: aged 40 to 80 without CVD, or any age with CVD.
        (population.has_cvd OR COALESCE(population.age BETWEEN 40 AND 80, FALSE)) AS is_statin_indicated
    FROM population
    -- The latest reading in the period is used; an invalid latest reading does not fall back.
    LEFT JOIN {{ nice_ref('int_nice_hba1c_evidence', reference) }} AS hba
        ON population.person_id = hba.person_id
        AND population.reporting_date = hba.reporting_date
        AND hba.latest_hba1c_date::DATE >= population.measurement_period_start
    LEFT JOIN {{ nice_ref('int_nice_blood_pressure_latest', reference) }} AS bp
        ON population.person_id = bp.person_id
        AND population.reporting_date = bp.reporting_date
        AND bp.latest_bp_date::DATE >= population.measurement_period_start
    LEFT JOIN {{ nice_ref('int_nice_therapy_evidence', reference) }} AS therapy
        ON population.person_id = therapy.person_id
        AND population.reporting_date = therapy.reporting_date
),

targets AS (
    SELECT
        *,
        COALESCE(is_latest_hba1c_valid AND latest_hba1c_value <= 58, FALSE) AS hba1c_in_target_range,
        COALESCE(is_valid_bp AND latest_systolic_value <= 140 AND latest_diastolic_value <= 80, FALSE)
            AS bp_in_target_range,
        -- Null when a statin is not indicated, so not applicable stays distinct from not met.
        CASE WHEN is_statin_indicated THEN is_statin_prescribed_in_period END AS is_statin_target_met
    FROM assessed
)

SELECT
    person_id,
    reporting_date,
    measurement_period_start,
    age,
    {{ nice_practice_columns('targets', reference) }},
    diabetes_type,
    CASE WHEN diabetes_type = 'Type 1' THEN 'Type 1' ELSE 'Type 2 and other' END AS nda_diabetes_type,
    latest_hba1c_date,
    latest_hba1c_value,
    is_latest_hba1c_valid,
    is_hba1c_recorded_in_period,
    hba1c_in_target_range,
    latest_bp_date,
    latest_systolic_value,
    latest_diastolic_value,
    is_valid_bp,
    is_bp_recorded_in_period,
    bp_in_target_range,
    has_cvd,
    is_statin_indicated,
    latest_statin_order_date,
    is_statin_prescribed_in_period,
    is_statin_target_met,
    hba1c_in_target_range
        AND bp_in_target_range
        AND COALESCE(is_statin_target_met, TRUE) AS all_three_targets_met
FROM targets
{% endmacro %}
