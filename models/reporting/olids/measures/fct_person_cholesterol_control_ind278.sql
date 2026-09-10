{{ config(materialized='table', cluster_by=['person_id']) }}

-- NICE IND278: https://www.nice.org.uk/indicators/ind278
WITH eligible_people AS (
    SELECT
        cvd.person_id,
        active.current_practice_code,
        active.current_practice_name,
        age.age,
        cvd.has_chd,
        cvd.has_stroke_tia,
        cvd.has_pad
    FROM {{ ref('int_cvd_secondary_prevention_population') }} cvd
    INNER JOIN {{ ref('dim_person_active_patients') }} active
        ON cvd.person_id = active.person_id
    LEFT JOIN {{ ref('dim_person_age') }} age
        ON cvd.person_id = age.person_id
    -- Both exclusions apply to the whole person, even with overlapping CVD diagnoses.
    WHERE NOT cvd.has_familial_hypercholesterolaemia
        AND NOT cvd.has_haemorrhagic_stroke
),

lipid_results AS (
    SELECT
        lipid.person_id,
        lipid.id AS observation_id,
        lipid.clinical_effective_date,
        lipid.cholesterol_value,
        lipid.is_valid_cholesterol,
        lipid.unit_status,
        lipid.recorded_value,
        lipid.source_result_unit_display,
        lipid.mapped_result_unit_display,
        lipid.conversion_factor,
        lipid.plausibility_status,
        lipid.is_lipid_review_required,
        'LDL cholesterol' AS lipid_type,
        1 AS same_day_priority,
        2.0 AS indicator_threshold
    FROM {{ ref('int_cholesterol_ldl_all') }} lipid
    INNER JOIN eligible_people eligible ON lipid.person_id = eligible.person_id
    WHERE lipid.clinical_effective_date::DATE
        BETWEEN DATEADD(month, -12, CURRENT_DATE()) AND CURRENT_DATE()

    UNION ALL

    SELECT
        lipid.person_id,
        lipid.id,
        lipid.clinical_effective_date,
        lipid.cholesterol_value,
        lipid.is_valid_cholesterol,
        lipid.unit_status,
        lipid.recorded_value,
        lipid.source_result_unit_display,
        lipid.mapped_result_unit_display,
        lipid.conversion_factor,
        lipid.plausibility_status,
        lipid.is_lipid_review_required,
        'Non-HDL cholesterol',
        2,
        2.6
    FROM {{ ref('int_cholesterol_non_hdl_all') }} lipid
    INNER JOIN eligible_people eligible ON lipid.person_id = eligible.person_id
    WHERE lipid.clinical_effective_date::DATE
        BETWEEN DATEADD(month, -12, CURRENT_DATE()) AND CURRENT_DATE()
),

last_recorded_result AS (
    SELECT *
    FROM lipid_results
    -- IND278 uses the last recorded result. Invalid evidence cannot be replaced by an older success.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id
        ORDER BY clinical_effective_date::DATE DESC, same_day_priority,
            clinical_effective_date DESC, observation_id DESC
    ) = 1
)

SELECT
    eligible.person_id,
    'IND278' AS indicator_id,
    'Cardiovascular disease prevention: cholesterol treatment target (secondary prevention)' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -12, CURRENT_DATE()) AS measurement_period_start,
    eligible.age,
    eligible.current_practice_code,
    eligible.current_practice_name,
    eligible.has_chd,
    eligible.has_stroke_tia,
    eligible.has_pad,
    result.observation_id AS latest_lipid_observation_id,
    result.clinical_effective_date AS latest_lipid_date,
    result.lipid_type,
    result.cholesterol_value AS latest_lipid_value,
    result.unit_status,
    result.recorded_value AS latest_lipid_recorded_value,
    result.source_result_unit_display,
    result.mapped_result_unit_display,
    result.conversion_factor,
    result.plausibility_status,
    result.is_lipid_review_required AS is_latest_lipid_review_required,
    result.indicator_threshold,
    'mmol/L' AS threshold_unit,
    TRUE AS is_in_denominator,
    result.observation_id IS NOT NULL AS is_lipid_recorded_in_last_12m,
    COALESCE(result.is_valid_cholesterol, FALSE) AS is_latest_lipid_valid,
    COALESCE(result.is_valid_cholesterol
        AND result.cholesterol_value <= result.indicator_threshold, FALSE) AS is_in_numerator,
    CASE
        WHEN result.observation_id IS NULL THEN 'NOT_RECORDED_IN_PERIOD'
        WHEN NOT result.is_valid_cholesterol THEN 'NOT_ASSESSABLE'
        WHEN result.cholesterol_value <= result.indicator_threshold THEN 'ACHIEVED'
        ELSE 'ABOVE_TARGET'
    END AS indicator_status
FROM eligible_people eligible
LEFT JOIN last_recorded_result result ON eligible.person_id = result.person_id
