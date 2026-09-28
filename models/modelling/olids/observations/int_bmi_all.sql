{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All BMI measurements for adults aged 18+ including both recorded BMI values and calculated BMI from HEIGHT/WEIGHT.
Includes ALL persons (active, inactive, deceased) aged 18+ with basic validation (10-150 range).
Uses ethnicity-adjusted BMI categories per NICE NG246 for cardiometabolic risk populations.
Avoids calculating BMI on dates where recorded BMI already exists for the same person.
Age restriction: paediatric height and weight measurements (age_at_event < 18) are excluded at source so they never feed calculated BMI. Final join on dim_person_age further filters to currently-18+ persons as adult BMI categories are only clinically appropriate for ages 18+.
*/

WITH recorded_bmi AS (
    -- Recorded BMI observations from BMIVAL_COD cluster
    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.date_recorded,
        TRY_CAST(obs.result_value AS FLOAT) AS bmi_value,
        obs.result_unit_display,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        obs.result_value,
        'recorded' AS bmi_source

    FROM ({{ get_observations("'BMIVAL_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
    AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
      AND obs.result_value IS NOT NULL
      AND TRY_CAST(obs.result_value AS FLOAT) IS NOT NULL
      
),

height_measurements AS (
    -- Get height measurements in cm, deduplicated to one per person per date
    -- for deterministic ASOF JOIN behaviour
    SELECT
        obs.person_id,
        obs.clinical_effective_date,
        obs.id,
        TRY_CAST(obs.result_value AS FLOAT) AS height_cm,
        obs.result_unit_display AS height_unit
    FROM ({{ get_observations("'HEIGHT'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
    AND obs.clinical_effective_date <= CURRENT_DATE()
      AND obs.result_value IS NOT NULL
      AND TRY_CAST(obs.result_value AS FLOAT) IS NOT NULL
      AND TRY_CAST(obs.result_value AS FLOAT) BETWEEN 50 AND 250
      -- Adult heights only: paediatric measurements must not feed into adult BMI calculations
      AND obs.age_at_event >= 18
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY obs.person_id, obs.clinical_effective_date
        ORDER BY obs.id DESC
    ) = 1
),

weight_measurements AS (
    -- Get weight measurements in kg
    SELECT
        obs.person_id,
        obs.clinical_effective_date,
        obs.id,
        obs.date_recorded,
        TRY_CAST(obs.result_value AS FLOAT) AS weight_kg,
        obs.result_unit_display AS weight_unit
    FROM ({{ get_observations("'WEIGHT'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
    AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
      AND obs.result_value IS NOT NULL
      AND TRY_CAST(obs.result_value AS FLOAT) IS NOT NULL
      AND TRY_CAST(obs.result_value AS FLOAT) BETWEEN 10 AND 500  -- Valid weight range in kg
      -- Adult weights only: paediatric measurements must not feed into adult BMI calculations
      AND obs.age_at_event >= 18
),

calculated_bmi AS (
    -- Calculate BMI from weight + most recent prior height using ASOF JOIN
    -- This avoids the explosive inequality join that produced billions of intermediate rows
    SELECT
        w.id AS ID,
        w.person_id,
        w.clinical_effective_date,
        w.date_recorded,
        ROUND(w.weight_kg / ((h.height_cm / 100.0) * (h.height_cm / 100.0)), 2) AS bmi_value,
        'kg/m²' AS result_unit_display,
        'CALCULATED_BMI' AS concept_code,
        'Calculated BMI from Height/Weight' AS concept_display,
        'CALCULATED' AS source_cluster_id,
        CAST(ROUND(w.weight_kg / ((h.height_cm / 100.0) * (h.height_cm / 100.0)), 2) AS VARCHAR(20)) AS result_value,
        'calculated' AS bmi_source
    FROM weight_measurements w
    ASOF JOIN height_measurements h
        MATCH_CONDITION (w.clinical_effective_date >= h.clinical_effective_date)
        ON w.person_id = h.person_id
    WHERE h.height_cm > 0
      -- Only calculate BMI for dates without existing recorded BMI for same person
      AND NOT EXISTS (
          SELECT 1 FROM recorded_bmi rb
          WHERE rb.person_id = w.person_id
            AND rb.clinical_effective_date = w.clinical_effective_date
      )
),

all_bmi AS (
    -- Combine recorded and calculated BMI
    SELECT * FROM recorded_bmi
    UNION ALL
    SELECT * FROM calculated_bmi
),

bmi_with_ethnicity AS (
    -- Join BMI data with ethnicity cardiometabolic risk information and age
    -- Filter to adults aged 18+ as pediatric BMI requires different percentile-based assessment
    SELECT
        ab.*,
        ecr.requires_lower_bmi_thresholds,
        ecr.cardiometabolic_risk_ethnicity_group,
        age.age
    FROM all_bmi ab
    INNER JOIN {{ ref('dim_person_age') }} age
        ON ab.person_id = age.person_id
        AND age.age >= 18  -- Adults only - pediatric BMI uses age/gender-specific percentiles
    LEFT JOIN {{ ref('int_ethnicity_cardiometabolic_risk') }} ecr
        ON ab.person_id = ecr.person_id
)

SELECT
    person_id,
    ID,
    clinical_effective_date,
    date_recorded,
    bmi_value,
    result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    result_value,
    bmi_source,

    -- Age information
    age,

    -- Ethnicity information
    requires_lower_bmi_thresholds,
    cardiometabolic_risk_ethnicity_group,

    -- Data quality validation
    CASE
        WHEN bmi_value BETWEEN 10 AND 150 THEN TRUE
        ELSE FALSE
    END AS is_valid_bmi,

    -- BMI categorisation (ethnicity-adjusted per NICE guidance)
    CASE
        WHEN bmi_value NOT BETWEEN 10 AND 150 THEN 'Invalid'
        WHEN bmi_value < 18.5 THEN 'Underweight'
        WHEN requires_lower_bmi_thresholds = TRUE THEN
            CASE
                WHEN bmi_value < 23 THEN 'Normal'
                WHEN bmi_value < 27.5 THEN 'Overweight'
                WHEN bmi_value < 32.5 THEN 'Obese Class I'
                WHEN bmi_value < 37.5 THEN 'Obese Class II'
                ELSE 'Obese Class III'
            END
        ELSE  -- Standard thresholds for other populations
            CASE
                WHEN bmi_value < 25 THEN 'Normal'
                WHEN bmi_value < 30 THEN 'Overweight'
                WHEN bmi_value < 35 THEN 'Obese Class I'
                WHEN bmi_value < 40 THEN 'Obese Class II'
                ELSE 'Obese Class III'
            END
    END AS bmi_category,

    -- BMI risk sort key (ethnicity-adjusted, higher number = higher risk)
    CASE
        WHEN bmi_value NOT BETWEEN 10 AND 150 THEN 0  -- Invalid
        WHEN bmi_value < 18.5 THEN 2  -- Underweight - Health risk
        WHEN requires_lower_bmi_thresholds = TRUE THEN
            CASE
                WHEN bmi_value < 23 THEN 1  -- Normal - Baseline/lowest risk
                WHEN bmi_value < 27.5 THEN 3  -- Overweight - Moderate risk
                WHEN bmi_value < 32.5 THEN 4  -- Obese Class I - High risk
                WHEN bmi_value < 37.5 THEN 5  -- Obese Class II - Higher risk
                ELSE 6  -- Obese Class III - Highest risk
            END
        ELSE  -- Standard thresholds for other populations
            CASE
                WHEN bmi_value < 25 THEN 1  -- Normal - Baseline/lowest risk
                WHEN bmi_value < 30 THEN 3  -- Overweight - Moderate risk
                WHEN bmi_value < 35 THEN 4  -- Obese Class I - High risk
                WHEN bmi_value < 40 THEN 5  -- Obese Class II - Higher risk
                ELSE 6  -- Obese Class III - Highest risk
            END
    END AS bmi_risk_sort_key

FROM bmi_with_ethnicity
