{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

WITH observations AS (
    {{ get_observations("'WAIST_COD', 'HEIGHT'") }}
),

measurements AS (
    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.date_recorded,
        obs.age_at_event,
        obs.cluster_id AS source_cluster_id,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.result_value AS original_result_value,
        COALESCE(NULLIF(TRIM(obs.result_unit_display), ''), units.display) AS original_result_unit_display,
        LOWER(REPLACE(COALESCE(NULLIF(TRIM(obs.result_unit_code), ''),
            original_result_unit_display), ' ', '')) AS unit_label,
        -- Only explicit length units can supply a ratio; do not infer centimetres from magnitude.
        TRY_TO_DECIMAL(obs.result_value::VARCHAR, 28, 9) * CASE
            WHEN unit_label IN ('cm', 'centimetre', 'centimetres', 'centimeter', 'centimeters') THEN 1
            WHEN unit_label IN ('m', 'metre', 'metres', 'meter', 'meters') THEN 100
            WHEN unit_label IN ('mm', 'millimetre', 'millimetres', 'millimeter', 'millimeters') THEN 0.1
            WHEN unit_label IN ('in', '[in_i]', 'inch', 'inches') THEN 2.54
        END AS value_cm
    FROM observations obs
    LEFT JOIN {{ ref('stg_olids_concept') }} units
        ON obs.result_units_source_concept_id = units.concept_id
    WHERE obs.clinical_effective_date_raw IS NOT NULL
        AND obs.clinical_effective_date::DATE <= CURRENT_DATE()
        AND TRY_TO_DECIMAL(obs.result_value::VARCHAR, 28, 9) IS NOT NULL
),

waist_people AS (
    SELECT DISTINCT person_id
    FROM measurements
    WHERE source_cluster_id = 'WAIST_COD'
),

heights AS (
    SELECT id, person_id, clinical_effective_date, date_recorded, value_cm AS height_cm
    FROM measurements
    WHERE source_cluster_id = 'HEIGHT'
        AND person_id IN (SELECT person_id FROM waist_people)
        AND age_at_event >= 18
        AND value_cm BETWEEN 50 AND 250
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id, clinical_effective_date::DATE
        ORDER BY date_recorded DESC NULLS LAST, id DESC
    ) = 1
),

waist_with_height AS (
    SELECT
        waist.*,
        height.height_cm,
        height.clinical_effective_date AS height_measurement_date
    FROM measurements waist
    LEFT JOIN heights height
        ON waist.person_id = height.person_id
        AND waist.age_at_event >= 18
        AND waist.value_cm BETWEEN 30 AND 250
        -- Local adult pairing window, inclusive, in either direction.
        AND height.clinical_effective_date::DATE BETWEEN
            DATEADD(day, -365, waist.clinical_effective_date::DATE)
            AND DATEADD(day, 365, waist.clinical_effective_date::DATE)
    WHERE waist.source_cluster_id = 'WAIST_COD'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY waist.id
        ORDER BY ABS(DATEDIFF(day, waist.clinical_effective_date, height.clinical_effective_date)),
            height.clinical_effective_date::DATE DESC,
            height.date_recorded DESC NULLS LAST, height.id DESC
    ) = 1
),

valid_bmi AS (
    SELECT person_id, id, clinical_effective_date, date_recorded, bmi_value
    FROM {{ ref('int_bmi_all') }}
    WHERE is_valid_bmi
        AND person_id IN (SELECT person_id FROM waist_people)
        AND clinical_effective_date::DATE <= CURRENT_DATE()
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id, clinical_effective_date::DATE
        ORDER BY IFF(bmi_source = 'recorded', 0, 1), date_recorded DESC NULLS LAST, id DESC
    ) = 1
),

paired AS (
    SELECT
        waist.*,
        bmi.bmi_value AS bmi_value_at_waist,
        bmi.clinical_effective_date AS bmi_measurement_date,
        waist.value_cm / NULLIF(waist.height_cm, 0) AS waist_to_height_ratio
    FROM waist_with_height waist
    LEFT JOIN valid_bmi bmi
        ON waist.person_id = bmi.person_id
        AND waist.age_at_event >= 18
        AND bmi.clinical_effective_date::DATE BETWEEN
            DATEADD(day, -90, waist.clinical_effective_date::DATE)
            AND DATEADD(day, 90, waist.clinical_effective_date::DATE)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY waist.id
        ORDER BY ABS(DATEDIFF(day, waist.clinical_effective_date, bmi.clinical_effective_date)),
            bmi.clinical_effective_date::DATE DESC,
            bmi.date_recorded DESC NULLS LAST, bmi.id DESC
    ) = 1
)

SELECT
    person_id,
    id,
    clinical_effective_date,
    date_recorded,
    value_cm::NUMBER(10,2) AS waist_circumference_value,
    IFF(value_cm IS NOT NULL, 'cm', original_result_unit_display) AS result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    original_result_value,
    original_result_unit_display,
    COALESCE(value_cm BETWEEN 30 AND 250, FALSE) AS is_valid_waist_circumference,
    height_cm::FLOAT AS height_cm,
    height_measurement_date,
    bmi_value_at_waist,
    bmi_measurement_date,
    waist_to_height_ratio::FLOAT AS waist_to_height_ratio,
    -- NG246 1.9.14 applies below BMI 35 and gives no healthy band below ratio 0.4.
    CASE WHEN bmi_value_at_waist < 35 THEN CASE
        -- Compare fixed-point lengths so equivalent units agree at exact thresholds.
        WHEN value_cm >= paired.height_cm * 0.6 THEN 'High central adiposity'
        WHEN value_cm >= paired.height_cm * 0.5 THEN 'Increased central adiposity'
        WHEN value_cm >= paired.height_cm * 0.4 THEN 'Healthy central adiposity'
    END END AS waist_to_height_ratio_category
FROM paired
