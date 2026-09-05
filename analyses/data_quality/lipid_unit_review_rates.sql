-- Aggregate screening rates, not a validated error rate. History counts observations;
-- latest populations count people with a recorded result for that analyte.
WITH observations AS (
    SELECT 'Total cholesterol' AS analyte, id, person_id, clinical_effective_date, unit_status,
        plausibility_status, is_unit_metadata_conflict, is_lipid_review_required
    FROM {{ ref('int_cholesterol_all') }}

    UNION ALL

    SELECT 'LDL' AS analyte, id, person_id, clinical_effective_date, unit_status,
        plausibility_status, is_unit_metadata_conflict, is_lipid_review_required
    FROM {{ ref('int_cholesterol_ldl_all') }}

    UNION ALL

    SELECT 'HDL' AS analyte, id, person_id, clinical_effective_date, unit_status,
        plausibility_status, is_unit_metadata_conflict, is_lipid_review_required
    FROM {{ ref('int_cholesterol_hdl_all') }}

    UNION ALL

    SELECT 'Non-HDL' AS analyte, id, person_id, clinical_effective_date, unit_status,
        plausibility_status, is_unit_metadata_conflict, is_lipid_review_required
    FROM {{ ref('int_cholesterol_non_hdl_all') }}

    UNION ALL

    SELECT 'Triglycerides' AS analyte, id, person_id, clinical_effective_date, unit_status,
        plausibility_status, is_unit_metadata_conflict, is_lipid_review_required
    FROM {{ ref('int_triglycerides_all') }}

    UNION ALL

    SELECT 'Total:HDL ratio' AS analyte, id, person_id, clinical_effective_date, unit_status,
        plausibility_status, is_unit_metadata_conflict, is_lipid_review_required
    FROM {{ ref('int_cholesterol_hdl_ratio_all') }}
),

latest_active AS (
    SELECT o.*
    FROM observations o
    INNER JOIN {{ ref('dim_person_active_patients') }} active ON o.person_id = active.person_id
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY o.analyte, o.person_id
        ORDER BY o.clinical_effective_date DESC, o.id DESC
    ) = 1
),

populations AS (
    SELECT 'All history' AS population, * FROM observations
    UNION ALL
    SELECT 'Latest recorded, active people', * FROM latest_active
    UNION ALL
    SELECT 'Latest recorded within 12 months, active people', * FROM latest_active
    WHERE clinical_effective_date::DATE BETWEEN DATEADD(month, -12, CURRENT_DATE()) AND CURRENT_DATE()
)

SELECT
    population,
    analyte,
    COUNT(*) AS result_count,
    COUNT_IF(is_lipid_review_required) AS review_count,
    ROUND(100.0 * COUNT_IF(is_lipid_review_required) / COUNT(*), 3) AS review_pct,
    ROUND(100.0 * COUNT_IF(unit_status IN ('Missing unit', 'Unsupported unit')) / COUNT(*), 3) AS unconvertible_pct,
    ROUND(100.0 * COUNT_IF(unit_status = 'Converted') / COUNT(*), 3) AS non_standard_conversion_pct,
    ROUND(100.0 * COUNT_IF(is_unit_metadata_conflict) / COUNT(*), 3) AS conflicting_unit_metadata_pct,
    ROUND(100.0 * COUNT_IF(plausibility_status NOT IN ('Within review range', 'Unit cannot be converted')) / COUNT(*), 3) AS numeric_review_pct
FROM populations
GROUP BY population, analyte
HAVING COUNT(DISTINCT person_id) >= 100
ORDER BY population, analyte
