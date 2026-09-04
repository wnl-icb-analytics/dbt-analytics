{{ config(materialized='table', cluster_by=['person_id']) }}

-- NICE IND278: https://www.nice.org.uk/indicators/ind278
WITH cvd_registers AS (
    SELECT person_id, 'CHD' AS condition
    FROM {{ ref('fct_person_chd_register') }}
    WHERE is_on_register AND earliest_diagnosis_date::DATE <= CURRENT_DATE()

    UNION ALL

    SELECT person_id, 'Stroke/TIA' AS condition
    FROM {{ ref('fct_person_stroke_tia_register') }}
    WHERE is_on_register AND earliest_diagnosis_date::DATE <= CURRENT_DATE()

    UNION ALL

    SELECT person_id, 'PAD' AS condition
    FROM {{ ref('fct_person_pad_register') }}
    WHERE is_on_register AND earliest_diagnosis_date::DATE <= CURRENT_DATE()
),

cvd_people AS (
    SELECT
        person_id,
        BOOLOR_AGG(condition = 'CHD') AS has_chd,
        BOOLOR_AGG(condition = 'Stroke/TIA') AS has_stroke_tia,
        BOOLOR_AGG(condition = 'PAD') AS has_pad
    FROM cvd_registers
    GROUP BY person_id
),

eligible_people AS (
    SELECT
        cvd.person_id,
        active.current_practice_code,
        active.current_practice_name,
        cvd.has_chd,
        cvd.has_stroke_tia,
        cvd.has_pad
    FROM cvd_people cvd
    INNER JOIN {{ ref('dim_person_active_patients') }} active
        ON cvd.person_id = active.person_id
    -- Both exclusions apply to the whole person, even with overlapping CVD diagnoses.
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ ref('int_familial_hypercholesterolaemia_diagnoses_all') }} fh
        WHERE fh.person_id = cvd.person_id
            AND fh.is_diagnosis_code
            AND (fh.clinical_effective_date_raw IS NULL
                OR fh.clinical_effective_date_raw::DATE <= CURRENT_DATE())
    )
        AND NOT EXISTS (
            SELECT 1
            FROM {{ ref('int_haemorrhagic_stroke_diagnoses_all') }} stroke
            WHERE stroke.person_id = cvd.person_id
                AND (stroke.clinical_effective_date_raw IS NULL
                    OR stroke.clinical_effective_date_raw::DATE <= CURRENT_DATE())
        )
),

lipid_results AS (
    SELECT
        lipid.person_id,
        lipid.id AS observation_id,
        lipid.clinical_effective_date,
        lipid.cholesterol_value,
        lipid.is_valid_cholesterol,
        lipid.unit_status,
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
    result.indicator_threshold,
    'mmol/L' AS threshold_unit,
    TRUE AS is_in_denominator,
    result.observation_id IS NOT NULL AS is_lipid_recorded_in_last_12m,
    COALESCE(result.is_valid_cholesterol, FALSE) AS is_latest_lipid_valid,
    COALESCE(result.is_valid_cholesterol
        AND result.cholesterol_value <= result.indicator_threshold, FALSE) AS is_in_numerator,
    CASE
        WHEN result.observation_id IS NULL THEN 'No lipid result in the preceding 12 months'
        WHEN NOT result.is_valid_cholesterol THEN 'Latest lipid result cannot be assessed'
        WHEN result.cholesterol_value <= result.indicator_threshold THEN 'Achieved'
        ELSE 'Latest lipid result above target'
    END AS indicator_status
FROM eligible_people eligible
LEFT JOIN last_recorded_result result ON eligible.person_id = result.person_id
