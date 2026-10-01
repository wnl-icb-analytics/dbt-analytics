{{
    config(
        materialized='table',
        cluster_by=['end_date', 'person_id'],
        tags=['monthly-full']
    )
}}

-- Monthly population state and segment assignment for the last 60 months.

WITH segment_inputs AS (
    SELECT
        pm.person_id,
        pm.sk_patient_id,
        pm.month_end_date AS end_date,

        pm.is_born,
        pm.is_alive,
        pm.is_registered,
        pm.is_active,
        pm.is_deceased,
        pm.age,
        pm.gender,
        pm.practice_code,
        pm.practice_name,
        pm.borough_registered,
        pm.neighbourhood_registered,

        CASE
            WHEN pm.is_active THEN COALESCE(
                c.is_on_palliative_care_register,
                FALSE
            )
        END AS is_end_of_life,
        CASE
            WHEN pm.is_active THEN ca.person_id IS NOT NULL
        END AS is_complex_adult,
        CASE
            WHEN pm.is_active THEN COALESCE(
                pm.age >= 18 AND l.ltc_count >= 2,
                FALSE
            )
        END AS has_multiple_ltcs,
        CASE
            WHEN pm.is_active THEN COALESCE(
                pm.age >= 18 AND l.ltc_count = 1,
                FALSE
            )
        END AS has_single_ltc,
        CASE
            WHEN pm.is_active THEN cc.person_id IS NOT NULL
        END AS is_complex_child,
        CASE
            WHEN pm.is_active THEN COALESCE(
                pm.age < 18 AND l.ltc_count >= 1,
                FALSE
            )
        END AS has_child_health_needs,

        IFF(pm.is_active, ZEROIFNULL(l.ltc_count), NULL) AS ltc_count,
        IFF(pm.is_active, ca.complexity_criteria_count, NULL)
            AS adult_complexity_criteria_count,
        -- int_segmentation_complex_children_history keeps only children
        -- meeting at least one criterion, so an active child missing from it
        -- has met none. Zero-fill inside the child cohort to match
        -- fct_person_segment, where every under-18 is retained.
        IFF(
            pm.is_active AND pm.age < 18,
            ZEROIFNULL(cc.complexity_criteria_count),
            NULL
        ) AS child_complexity_criteria_count,

        cv.community_latest_activity_date,
        cv.is_community_window_complete,
        cv.community_lag_days
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    LEFT JOIN {{ ref('int_segmentation_clinical_status_history') }} AS c
        ON pm.person_id = c.person_id
        AND pm.month_end_date = c.end_date
    LEFT JOIN {{ ref('int_segmentation_complex_adults_history') }} AS ca
        ON pm.person_id = ca.person_id
        AND pm.month_end_date = ca.end_date
    LEFT JOIN {{ ref('int_segmentation_ltc_count_history') }} AS l
        ON pm.person_id = l.person_id
        AND pm.month_end_date = l.end_date
    LEFT JOIN {{ ref('int_segmentation_complex_children_history') }} AS cc
        ON pm.person_id = cc.person_id
        AND pm.month_end_date = cc.end_date
    INNER JOIN {{ ref('int_segmentation_service_activity_coverage_history') }} AS cv
        ON pm.month_end_date = cv.end_date
),

assigned AS (
    SELECT
        *,
        CASE
            WHEN NOT is_active THEN NULL
            WHEN is_end_of_life THEN 8
            WHEN age >= 18 AND is_complex_adult THEN 7
            WHEN has_multiple_ltcs THEN 6
            WHEN has_single_ltc THEN 5
            WHEN age >= 18 THEN 4
            WHEN is_complex_child THEN 3
            WHEN has_child_health_needs THEN 2
            ELSE 1
        END AS segment_number
    FROM segment_inputs
),

named AS (
    SELECT
        *,
        CASE segment_number
            WHEN 8 THEN 'End of Life'
            WHEN 7 THEN 'Adults with Complexity'
            WHEN 6 THEN 'Adults with Multiple LTCs'
            WHEN 5 THEN 'Adults with a Single LTC'
            WHEN 4 THEN 'Mostly Healthy Adults'
            WHEN 3 THEN 'Children with Complexity'
            WHEN 2 THEN 'Children with Health Needs'
            WHEN 1 THEN 'Mostly Healthy Children'
        END AS segment_name
    FROM assigned
)

SELECT
    person_id,
    sk_patient_id,
    end_date,
    segment_number,
    segment_name,

    is_born,
    is_alive,
    is_registered,
    is_active,
    is_deceased,
    age,
    gender,
    practice_code,
    practice_name,
    borough_registered,
    neighbourhood_registered,

    is_end_of_life,
    is_complex_adult,
    has_multiple_ltcs,
    has_single_ltc,
    is_complex_child,
    has_child_health_needs,
    ltc_count,
    adult_complexity_criteria_count,
    child_complexity_criteria_count,

    community_latest_activity_date,
    is_community_window_complete,
    community_lag_days
FROM named
