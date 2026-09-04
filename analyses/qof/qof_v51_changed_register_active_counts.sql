/*
Changed-register counts for external EMIS QOF v51 validation.

Active means alive, non-test and currently registered in
dim_person_active_patients on the query date. All NCL practices are on EMIS.
*/

WITH active_people AS (
    SELECT
        person_id,
        current_practice_code
    FROM {{ ref('dim_person_active_patients') }}
),

scope AS (
    SELECT
        CURRENT_DATE() AS count_date,
        COUNT(*) AS active_population,
        COUNT(DISTINCT current_practice_code) AS active_practices
    FROM active_people
),

hf_members AS (
    SELECT
        person_id,
        is_on_hfref_register
    FROM {{ ref('fct_person_heart_failure_register') }}
),

smi_lithium_only AS (
    SELECT programme.person_id
    FROM {{ ref('int_smi_population_base') }} AS programme
    LEFT JOIN {{ ref('fct_person_smi_register') }} AS register USING (person_id)
    WHERE programme.is_on_lithium = TRUE
        AND register.person_id IS NULL
),

previous_obesity_members AS (
    SELECT bmi.person_id
    FROM {{ ref('int_bmi_qof') }} AS bmi
    INNER JOIN {{ ref('dim_person_age') }} AS age USING (person_id)
    LEFT JOIN {{ ref('int_ethnicity_qof') }} AS ethnicity USING (person_id)
    WHERE age.age >= 18
        AND (
            bmi.is_bmi_30_plus = TRUE
            OR (
                COALESCE(ethnicity.is_bame, FALSE) = TRUE
                AND bmi.is_bmi_27_5_plus = TRUE
            )
        )
),

obesity_invalid_bmi_leavers AS (
    SELECT previous.person_id
    FROM previous_obesity_members AS previous
    LEFT JOIN {{ ref('fct_person_obesity_register') }} AS current_register
        USING (person_id)
    WHERE current_register.person_id IS NULL
),

cohort_rows AS (
    SELECT person_id, 'AST_REG' AS register_code,
        'Total register' AS breakdown, 'Register total' AS breakdown_type
    FROM {{ ref('fct_person_asthma_register') }}
    UNION ALL
    SELECT person_id, 'AST_REG', 'Age 5', 'v51 addition'
    FROM {{ ref('fct_person_asthma_register') }} WHERE age = 5
    UNION ALL
    SELECT person_id, 'AST_REG', 'Age 6 or over',
        'Mutually exclusive age remainder'
    FROM {{ ref('fct_person_asthma_register') }} WHERE age >= 6

    UNION ALL
    SELECT person_id, 'COPD_REG', 'Total register', 'Register total'
    FROM {{ ref('fct_person_copd_register') }}
    UNION ALL
    SELECT person_id, 'COPD_REG', qof_rule_applied, 'Mutually exclusive rule'
    FROM {{ ref('fct_person_copd_register') }}
    UNION ALL
    SELECT
        person_id,
        'COPD_REG',
        CASE
            WHEN copd_disorder_code_count > 0
                AND copd_admin_code_count = 0 THEN 'Disorder evidence only'
            WHEN copd_disorder_code_count = 0
                AND copd_admin_code_count > 0 THEN 'Administrative evidence only'
            ELSE 'Disorder and administrative evidence'
        END,
        'Mutually exclusive evidence route'
    FROM {{ ref('fct_person_copd_register') }}

    UNION ALL
    SELECT person_id, 'NDH_REG', 'Total QOF NDH/GDM register', 'Register total'
    FROM {{ ref('fct_person_qof_ndh_gdm_register') }}
    UNION ALL
    SELECT
        person_id,
        'NDH_REG',
        CASE
            WHEN has_ndh_route AND has_gdm_route THEN 'NDH and GDM routes'
            WHEN has_ndh_route THEN 'NDH route only'
            ELSE 'GDM route only'
        END,
        'Mutually exclusive entry route'
    FROM {{ ref('fct_person_qof_ndh_gdm_register') }}
    UNION ALL
    SELECT person_id, 'NDH_REG', 'Qualifying rule ' || qualifying_rule,
        'Mutually exclusive rule'
    FROM {{ ref('fct_person_qof_ndh_gdm_register') }}
    UNION ALL
    SELECT person_id, 'NDH clinical', 'NDH/IGT/PRD-only cohort',
        'Population-health comparator'
    FROM {{ ref('fct_person_ndh_register') }}

    UNION ALL
    SELECT person_id, 'HF1_REG', 'Total unresolved heart failure', 'Register total'
    FROM hf_members
    UNION ALL
    SELECT person_id, 'HF3_REG', 'Reduced ejection fraction', 'v51 sub-register'
    FROM hf_members WHERE is_on_hfref_register = TRUE
    UNION ALL
    SELECT person_id, 'HF1_REG', 'HF1 members outside HF3',
        'Mutually exclusive remainder'
    FROM hf_members WHERE is_on_hfref_register = FALSE

    UNION ALL
    SELECT person_id, 'MH1_REG', 'Diagnosis-qualified register', 'Register total'
    FROM {{ ref('fct_person_smi_register') }}
    UNION ALL
    SELECT person_id, 'MH1_REG', 'Diagnosis-qualified and on lithium',
        'Overlapping treatment group'
    FROM {{ ref('fct_person_smi_register') }} WHERE is_on_lithium = TRUE
    UNION ALL
    SELECT person_id, 'MH1_REG', 'Diagnosis-qualified without current lithium',
        'Mutually exclusive treatment remainder'
    FROM {{ ref('fct_person_smi_register') }}
    WHERE is_on_lithium = FALSE OR is_on_lithium IS NULL
    UNION ALL
    SELECT person_id, 'Former MH2_REG', 'Lithium-only members excluded from v51',
        'Current-active comparison only'
    FROM smi_lithium_only

    UNION ALL
    SELECT person_id, 'OB_REG', 'Total register', 'Register total'
    FROM {{ ref('fct_person_obesity_register') }}
    UNION ALL
    SELECT
        person_id,
        'OB_REG',
        CASE
            WHEN has_bmi_30_plus THEN 'BMI 30 or over route'
            ELSE 'Lower ethnicity threshold only'
        END,
        'Mutually exclusive BMI route'
    FROM {{ ref('fct_person_obesity_register') }}
    UNION ALL
    SELECT person_id, 'OB_REG', 'Invalid-BMI members removed by fix',
        'Implementation correction'
    FROM obesity_invalid_bmi_leavers

    UNION ALL
    SELECT person_id, 'CD_REG', 'Total CVD register', 'Register total'
    FROM {{ ref('fct_person_cvd_register') }}
    UNION ALL
    SELECT
        person_id,
        'CD_REG',
        CASE
            WHEN is_qualified_via_chd AND is_qualified_via_stroke_tia
                THEN 'CHD and stroke/TIA'
            WHEN is_qualified_via_chd THEN 'CHD only'
            ELSE 'Stroke/TIA only'
        END,
        'Mutually exclusive component route'
    FROM {{ ref('fct_person_cvd_register') }}

    UNION ALL
    SELECT person_id, 'OBES2_REG', 'Total register', 'Register total'
    FROM {{ ref('fct_person_obesity2_register') }}
    UNION ALL
    SELECT
        person_id,
        'OBES2_REG',
        CASE
            WHEN latest_bmi_35_date IS NOT NULL THEN 'BMI 35 or over route'
            ELSE 'Lower ethnicity threshold only'
        END,
        'Mutually exclusive BMI route'
    FROM {{ ref('fct_person_obesity2_register') }}
    UNION ALL
    SELECT person_id, 'OBES2_REG', comorbidity_count || ' qualifying comorbidities',
        'Mutually exclusive comorbidity count'
    FROM {{ ref('fct_person_obesity2_register') }}
    UNION ALL
    SELECT person_id, 'OBES2_REG', 'ASCVD', 'Overlapping comorbidity group'
    FROM {{ ref('fct_person_obesity2_register') }} WHERE has_ascvd
    UNION ALL
    SELECT person_id, 'OBES2_REG', 'Hypertension', 'Overlapping comorbidity group'
    FROM {{ ref('fct_person_obesity2_register') }} WHERE has_unresolved_hypertension
    UNION ALL
    SELECT person_id, 'OBES2_REG', 'Dyslipidaemia', 'Overlapping comorbidity group'
    FROM {{ ref('fct_person_obesity2_register') }} WHERE has_dyslipidaemia
    UNION ALL
    SELECT person_id, 'OBES2_REG', 'Obstructive sleep apnoea',
        'Overlapping comorbidity group'
    FROM {{ ref('fct_person_obesity2_register') }} WHERE has_obstructive_sleep_apnoea
    UNION ALL
    SELECT person_id, 'OBES2_REG', 'Type 2 diabetes', 'Overlapping comorbidity group'
    FROM {{ ref('fct_person_obesity2_register') }} WHERE has_unresolved_type2_diabetes
),

aggregated AS (
    SELECT
        cohort.register_code,
        cohort.breakdown,
        cohort.breakdown_type,
        COUNT(*) AS all_person_count,
        COUNT_IF(active.person_id IS NOT NULL) AS active_person_count
    FROM cohort_rows AS cohort
    LEFT JOIN active_people AS active USING (person_id)
    GROUP BY cohort.register_code, cohort.breakdown, cohort.breakdown_type
),

with_expected_zeroes AS (
    SELECT * FROM aggregated
    UNION ALL
    SELECT
        'COPD_REG',
        'Rule 3: Newly Registered + Spirometry',
        'Mutually exclusive rule',
        0,
        0
    WHERE NOT EXISTS (
        SELECT 1
        FROM aggregated
        WHERE register_code = 'COPD_REG'
            AND breakdown = 'Rule 3: Newly Registered + Spirometry'
    )
)

SELECT
    scope.count_date,
    scope.active_practices,
    scope.active_population,
    breakdown.register_code,
    breakdown.breakdown,
    breakdown.breakdown_type,
    breakdown.all_person_count,
    breakdown.active_person_count
FROM with_expected_zeroes AS breakdown
CROSS JOIN scope
ORDER BY breakdown.register_code, breakdown.breakdown_type, breakdown.breakdown
