{{
    config(
        tags=['qof', 'validation', 'emis']
    )
}}

/*
QOF Register Validation: EMIS Extract vs Internal pit_ Models

Detailed practice-level comparison showing all practices and registers.

Data Sources:
- EMIS: stg_reference_emis_qof_v50_register_counts (filtered to disease registers, mapped to OLIDS names)
- OLIDS: pit_ register models (2025-11-04 reference date)

Validation Criteria:
- Aggregate: within 1% across all practices
- By Practice: no more than 2% difference OR 5 patients difference

Usage:
    dbt compile --select compare_emis_qof_vs_pit_registers

Output:
- First rows: AGGREGATE comparison by register
- Remaining rows: PRACTICE-level comparison (failures first, then passes)
*/

WITH validated_practices AS (
    -- Practices with known good registration counts (133/175)
    SELECT practice_code
    FROM {{ ref('emis_olids_reg_pass_direct_care') }}
),

person_practices AS (
    -- Filter to patients actively registered at reference date, at validated practices only
    SELECT
        h.person_id,
        h.practice_code,
        h.practice_name
    FROM {{ ref('dim_person_demographics_historical') }} h
    INNER JOIN validated_practices vp ON h.practice_code = vp.practice_code
    WHERE h.effective_start_date <= '{{ var("qof_reference_date") }}'::DATE
      AND (h.effective_end_date IS NULL OR h.effective_end_date > '{{ var("qof_reference_date") }}'::DATE)
      AND h.is_active = TRUE
      AND (h.is_deceased = FALSE OR h.death_date_approx > '{{ var("qof_reference_date") }}'::DATE)
),

-- Get pit register data for each person
pit_diabetes AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_diabetes_register') }} v ON ar.person_id = v.person_id
),

pit_asthma AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_asthma_register') }} v ON ar.person_id = v.person_id
),

pit_chd AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_chd_register') }} v ON ar.person_id = v.person_id
),

pit_hypertension AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_hypertension_register') }} v ON ar.person_id = v.person_id
),

pit_copd AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_copd_register') }} v ON ar.person_id = v.person_id
),

pit_ckd AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_ckd_register') }} v ON ar.person_id = v.person_id
),

pit_atrial_fibrillation AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_atrial_fibrillation_register') }} v ON ar.person_id = v.person_id
),

pit_heart_failure AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_heart_failure_register') }} v ON ar.person_id = v.person_id
),

pit_stroke_tia AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_stroke_tia_register') }} v ON ar.person_id = v.person_id
),

pit_pad AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_pad_register') }} v ON ar.person_id = v.person_id
),

pit_dementia AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_dementia_register') }} v ON ar.person_id = v.person_id
),

pit_depression AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_depression_register') }} v ON ar.person_id = v.person_id
),

pit_smi AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_smi_register') }} v ON ar.person_id = v.person_id
),

pit_epilepsy AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_epilepsy_register') }} v ON ar.person_id = v.person_id
),

pit_cancer AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_cancer_register') }} v ON ar.person_id = v.person_id
),

pit_palliative_care AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_palliative_care_register') }} v ON ar.person_id = v.person_id
),

pit_learning_disability AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_learning_disability_register') }} v ON ar.person_id = v.person_id
),

pit_osteoporosis AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_osteoporosis_register') }} v ON ar.person_id = v.person_id
),

pit_rheumatoid_arthritis AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_rheumatoid_arthritis_register') }} v ON ar.person_id = v.person_id
),

pit_ndh AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_qof_ndh_gdm_register') }} v ON ar.person_id = v.person_id
),

pit_obesity AS (
    SELECT
        ar.person_id,
        ar.practice_code,
        v.register_name,
        v.is_on_register
    FROM person_practices ar
    LEFT JOIN {{ ref('pit_obesity_register') }} v ON ar.person_id = v.person_id
),

-- Aggregate pit counts by practice and register
pit_counts_by_practice AS (
    SELECT practice_code, 'Diabetes' AS register_name, SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) AS pit_count FROM pit_diabetes GROUP BY practice_code
    UNION ALL SELECT practice_code, 'Asthma', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM pit_asthma GROUP BY practice_code
    UNION ALL SELECT practice_code, 'CHD', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM pit_chd GROUP BY practice_code
    UNION ALL SELECT practice_code, 'Hypertension', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM pit_hypertension GROUP BY practice_code
    UNION ALL SELECT practice_code, 'COPD', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM pit_copd GROUP BY practice_code
    UNION ALL SELECT practice_code, 'CKD', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM pit_ckd GROUP BY practice_code
    UNION ALL SELECT practice_code, 'Atrial Fibrillation', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM pit_atrial_fibrillation GROUP BY practice_code
    UNION ALL SELECT practice_code, 'Heart Failure', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM pit_heart_failure GROUP BY practice_code
    UNION ALL SELECT practice_code, 'Stroke/TIA', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM pit_stroke_tia GROUP BY practice_code
    UNION ALL SELECT practice_code, 'PAD', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM pit_pad GROUP BY practice_code
    UNION ALL SELECT practice_code, 'Dementia', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM pit_dementia GROUP BY practice_code
    UNION ALL SELECT practice_code, 'Depression', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM pit_depression GROUP BY practice_code
    UNION ALL SELECT practice_code, 'SMI', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM pit_smi GROUP BY practice_code
    UNION ALL SELECT practice_code, 'Epilepsy', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM pit_epilepsy GROUP BY practice_code
    UNION ALL SELECT practice_code, 'Cancer', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM pit_cancer GROUP BY practice_code
    UNION ALL SELECT practice_code, 'Palliative Care', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM pit_palliative_care GROUP BY practice_code
    UNION ALL SELECT practice_code, 'Learning Disability', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM pit_learning_disability GROUP BY practice_code
    UNION ALL SELECT practice_code, 'Osteoporosis', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM pit_osteoporosis GROUP BY practice_code
    UNION ALL SELECT practice_code, 'Rheumatoid Arthritis', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM pit_rheumatoid_arthritis GROUP BY practice_code
    UNION ALL SELECT practice_code, 'NDH', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM pit_ndh GROUP BY practice_code
    UNION ALL SELECT practice_code, 'Obesity', SUM(CASE WHEN is_on_register THEN 1 ELSE 0 END) FROM pit_obesity GROUP BY practice_code
),

-- Get EMIS counts (filtered to validated practices only)
emis_counts_by_practice AS (
    SELECT
        e.practice_code,
        e.register_name,
        e.population_count AS emis_count
    FROM {{ ref('stg_reference_emis_qof_v50_register_counts') }} e
    INNER JOIN validated_practices vp ON e.practice_code = vp.practice_code
),

-- Practice-level comparison
practice_comparison AS (
    SELECT
        COALESCE(e.practice_code, p.practice_code) AS practice_code,
        COALESCE(e.register_name, p.register_name) AS register_name,
        COALESCE(e.emis_count, 0) AS emis_count,
        COALESCE(p.pit_count, 0) AS pit_count,
        COALESCE(e.emis_count, 0) - COALESCE(p.pit_count, 0) AS difference,
        ABS(COALESCE(e.emis_count, 0) - COALESCE(p.pit_count, 0)) AS abs_difference,
        ROUND(100.0 * (COALESCE(e.emis_count, 0) - COALESCE(p.pit_count, 0)) / NULLIF(COALESCE(e.emis_count, 0), 0), 2) AS pct_difference,
        CASE
            WHEN ABS(100.0 * (COALESCE(e.emis_count, 0) - COALESCE(p.pit_count, 0)) / NULLIF(COALESCE(e.emis_count, 0), 0)) <= 2 THEN TRUE
            WHEN ABS(COALESCE(e.emis_count, 0) - COALESCE(p.pit_count, 0)) <= 5 THEN TRUE
            ELSE FALSE
        END AS practice_pass
    FROM emis_counts_by_practice e
    FULL OUTER JOIN pit_counts_by_practice p
        ON e.practice_code = p.practice_code
        AND e.register_name = p.register_name
    WHERE e.register_name IS NOT NULL OR p.register_name IS NOT NULL
),

-- Aggregate comparison (1% threshold)
aggregate_comparison AS (
    SELECT
        register_name,
        SUM(emis_count) AS total_emis_count,
        SUM(pit_count) AS total_pit_count,
        SUM(emis_count) - SUM(pit_count) AS total_difference,
        ROUND(100.0 * (SUM(emis_count) - SUM(pit_count)) / NULLIF(SUM(emis_count), 0), 2) AS pct_difference,
        CASE
            WHEN ABS(100.0 * (SUM(emis_count) - SUM(pit_count)) / NULLIF(SUM(emis_count), 0)) <= 1 THEN 'PASS'
            ELSE 'FAIL'
        END AS aggregate_pass
    FROM practice_comparison
    GROUP BY register_name
)

-- Final output: both comparisons
SELECT
    'AGGREGATE' AS comparison_type,
    register_name,
    NULL AS practice_code,
    total_emis_count AS emis_count,
    total_pit_count AS pit_count,
    total_difference AS difference,
    pct_difference,
    aggregate_pass AS result
FROM aggregate_comparison

UNION ALL

SELECT
    'PRACTICE' AS comparison_type,
    register_name,
    practice_code,
    emis_count,
    pit_count,
    difference,
    pct_difference,
    CASE WHEN practice_pass THEN 'PASS' ELSE 'FAIL' END AS result
FROM practice_comparison

ORDER BY
    comparison_type DESC,
    register_name,
    CASE WHEN result = 'FAIL' THEN 0 ELSE 1 END,
    practice_code
