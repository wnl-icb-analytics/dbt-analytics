{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
BMI measurements with QOF-specific rules for obesity register eligibility.
Includes both numeric BMI values (BMIVAL_COD) and BMI30+ codes (BMI30_COD).
Implements specific obesity register logic with ethnicity-adjusted thresholds and BAME classification.
*/

WITH validated_observations AS (

    SELECT *
    FROM {{ ref('int_bmi_qof_all') }}
),

person_level_aggregation AS (

    SELECT
        person_id,

        -- Latest BMI information
        MAX(clinical_effective_date) AS latest_bmi_date,
        MAX(CASE WHEN is_valid_bmi THEN clinical_effective_date END) AS latest_valid_bmi_date,

        -- QOF flags based on any qualifying observation
        MAX(is_bmi_30_plus::int)::boolean AS has_bmi_30_plus_ever,
        MAX(is_bmi_27_5_plus::int)::boolean AS has_bmi_27_5_plus_ever,

        -- Aggregate concept information
        ARRAY_AGG(DISTINCT concept_code) AS all_bmi_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) AS all_bmi_concept_displays

    FROM validated_observations
    GROUP BY person_id
),

latest_valid_bmi AS (

    SELECT
        person_id,
        bmi_value AS latest_valid_bmi_value,
        ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC, id DESC) AS rn
    FROM validated_observations
    WHERE is_valid_bmi = TRUE
),

latest_observations AS (

    SELECT
        vo.*,
        ROW_NUMBER() OVER (PARTITION BY vo.person_id ORDER BY vo.clinical_effective_date DESC, vo.id DESC) AS rn
    FROM validated_observations vo
),

bmi_with_ethnicity AS (
    -- Join BMI data with ethnicity cardiometabolic risk information for BAME classification
    SELECT
        lo.*,
        ecr.requires_lower_bmi_thresholds,
        ecr.cardiometabolic_risk_ethnicity_group,
        
        -- BAME classification for QOF (True if requires lower BMI thresholds)
        COALESCE(ecr.requires_lower_bmi_thresholds, FALSE) AS is_bame,
        
        -- Ethnicity-adjusted BMI 27.5+ flag
        CASE
            WHEN ecr.requires_lower_bmi_thresholds = TRUE THEN
                CASE WHEN lo.bmi_value >= 27.5 THEN TRUE ELSE FALSE END
            ELSE 
                CASE WHEN lo.bmi_value >= 30 THEN TRUE ELSE FALSE END  -- Standard threshold for non-BAME
        END AS is_bmi_27_5_plus_ethnicity_adjusted
        
    FROM latest_observations lo
    LEFT JOIN {{ ref('int_ethnicity_cardiometabolic_risk') }} ecr
        ON lo.person_id = ecr.person_id
    WHERE lo.rn = 1
)

-- Final selection with person-level QOF flags and ethnicity data
SELECT
    bwe.person_id,
    bwe.id,
    bwe.clinical_effective_date,
    bwe.concept_code,
    bwe.concept_display,
    bwe.source_cluster_id,
    bwe.bmi_value,
    bwe.is_bmi_30_plus,
    bwe.is_bmi_27_5_plus,
    bwe.is_bmi_25_plus,
    bwe.is_valid_bmi,
    bwe.result_value,

    -- Ethnicity information
    bwe.requires_lower_bmi_thresholds,
    bwe.cardiometabolic_risk_ethnicity_group,
    bwe.is_bame,
    bwe.is_bmi_27_5_plus_ethnicity_adjusted,

    -- Person-level aggregated data
    pla.latest_bmi_date,
    pla.latest_valid_bmi_date,
    lvb.latest_valid_bmi_value,
    pla.has_bmi_30_plus_ever,
    pla.has_bmi_27_5_plus_ever,
    pla.all_bmi_concept_codes,
    pla.all_bmi_concept_displays

FROM bmi_with_ethnicity bwe
LEFT JOIN person_level_aggregation pla ON bwe.person_id = pla.person_id
LEFT JOIN latest_valid_bmi lvb ON bwe.person_id = lvb.person_id AND lvb.rn = 1
