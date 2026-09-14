{{ config(materialized='view') }}

-- NICE IND134: https://www.nice.org.uk/indicators/ind134
-- ACE inhibitor or ARB order in 6 months for diabetes with proteinuria or microalbuminuria; excludes contraindications to both classes.
WITH indicator_population AS (
    SELECT
        diabetes.person_id,
        age.age
    FROM {{ ref('fct_person_diabetes_register') }} AS diabetes
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON diabetes.person_id = age.person_id
    WHERE diabetes.is_on_register
        AND EXISTS (
            SELECT 1 FROM {{ ref('int_proteinuria_all') }} AS kidney
            WHERE kidney.person_id = diabetes.person_id
                AND kidney.source_cluster_id IN ('PRT_COD', 'MAL_COD')
        )
        -- NICE excludes people contraindicated to both an ACE inhibitor and an ARB: persisting at any time or expiring in 12 months
        AND NOT (
            EXISTS (
                SELECT 1 FROM {{ ref('int_ras_contraindication_all') }} AS contra
                WHERE contra.person_id = diabetes.person_id AND contra.drug_class = 'ACE_INHIBITOR'
                    AND (contra.is_persisting OR contra.clinical_effective_date::DATE >= DATEADD(month, -12, CURRENT_DATE()))
            )
            AND EXISTS (
                SELECT 1 FROM {{ ref('int_ras_contraindication_all') }} AS contra
                WHERE contra.person_id = diabetes.person_id AND contra.drug_class = 'ARB'
                    AND (contra.is_persisting OR contra.clinical_effective_date::DATE >= DATEADD(month, -12, CURRENT_DATE()))
            )
        )
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        ras.latest_order_date AS latest_therapy_order_date,
        ras.latest_ras_class AS latest_therapy_class,
        CASE WHEN ras.latest_order_date >= DATEADD(month, -6, CURRENT_DATE()) THEN ras.latest_order_date END AS latest_record_date,
        COALESCE(ras.latest_order_date >= DATEADD(month, -6, CURRENT_DATE()), FALSE) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
    LEFT JOIN {{ ref('int_renin_angiotensin_therapy_latest') }} AS ras
        ON population.person_id = ras.person_id
)

SELECT
    person_id,
    'IND134' AS indicator_id,
    'Diabetes: ACEi or ARBs' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -6, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Diabetes with proteinuria or microalbuminuria' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_therapy_order_date,
    latest_therapy_class,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        WHEN latest_therapy_order_date IS NULL THEN 'NEVER_TREATED'
        ELSE 'NOT_TREATED_IN_PERIOD'
    END AS indicator_status
FROM assessed
