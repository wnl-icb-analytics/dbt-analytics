-- Pair: macros/qof_registers/calculate_copd_register.sql.
-- This live fact includes future-dated records. Its PIT pair is strict as-of
-- and derives age at the reference date where age is used.

{{
    config(
        materialized='table',
        cluster_by=['person_id', 'is_on_register'])
}}

/*
COPD Register - Official QOF v51 Business Rules Implementation

Implements the QOF COPD register specification v107 for 2026/27.

RULE 1: If EUNRESCOPD_DAT < 01/04/2023 → SELECT
- Automatic inclusion for patients with earliest unresolved diagnosis before April 2023

RULE 2: If EUNRESCOPD_DAT >= 01/04/2023 AND spirometry within timeframe → SELECT
- FEV1/FVC <0.7 within 93 days before and 186 days after EUNRESCOPD_DAT

RULE 3: If EUNRESCOPD_DAT >= 01/04/2023 AND newly registered (last 12 months) AND spirometry → SELECT
- FEV1/FVC <0.7 within 93 days before and 186 days after REG_DAT
- For patients who registered recently and already have spirometry from before registration

RULE 4: If EUNRESCOPD_DAT >= 01/04/2023 → SELECT (all remaining)
- All remaining post-2023 patients are included

QOF v51 diagnosis derivation:
- COPDDIAG_COD disorder evidence counts at any date up to the achievement date.
- COPDPROC_COD administrative evidence counts only in the preceding two years.
- COPDEAR_DAT is the earliest eligible disorder or administrative evidence.
- COPDRES_DAT is the latest resolved code up to the achievement date.
- COPDLAT_DAT is the earliest eligible evidence after COPDRES_DAT.
- EUNRESCOPD_DAT is COPDEAR_DAT when never resolved, otherwise COPDLAT_DAT.
*/

WITH eligible_copd_evidence AS (
    -- Disorder evidence is all-time; administrative evidence has an exclusive two-year cutoff.
    SELECT
        d.person_id,
        d.clinical_effective_date AS evidence_date,
        d.ID,
        d.concept_code,
        d.concept_display,
        d.source_cluster_id,
        d.is_disorder_code,
        d.is_admin_code,
        age.age
    FROM {{ ref('int_copd_diagnoses_all') }} AS d
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON d.person_id = age.person_id
    WHERE
        d.is_disorder_code = TRUE
        OR (
            d.is_admin_code = TRUE
            AND d.clinical_effective_date > DATEADD('year', -2, CURRENT_DATE())
        )
),

copd_resolved_codes AS (
    -- COPDRES_DAT: latest resolved code up to the achievement date.
    SELECT
        d.person_id,
        d.clinical_effective_date AS resolution_date,
        d.ID,
        d.concept_code,
        d.concept_display
    FROM {{ ref('int_copd_diagnoses_all') }} AS d
    WHERE d.is_resolved_code = TRUE
),

person_evidence_aggregates AS (
    SELECT
        person_id,
        MIN(evidence_date) AS copdear_dat,
        MAX(evidence_date) AS latest_evidence_date,
        COUNT(DISTINCT ID) AS copd_diagnosis_count,
        COUNT(DISTINCT CASE WHEN is_disorder_code THEN ID END) AS copd_disorder_code_count,
        COUNT(DISTINCT CASE WHEN is_admin_code THEN ID END) AS copd_admin_code_count,
        MAX(age) AS current_age,
        ARRAY_AGG(DISTINCT concept_code) AS all_copd_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) AS all_copd_concept_displays
    FROM eligible_copd_evidence
    GROUP BY person_id
),

person_resolved_aggregates AS (
    SELECT
        person_id,
        MAX(resolution_date) AS copdres_dat
    FROM copd_resolved_codes
    GROUP BY person_id
),

qof_field_calculations AS (
    SELECT
        pca.*,
        pra.copdres_dat
    FROM person_evidence_aggregates AS pca
    LEFT JOIN person_resolved_aggregates AS pra
        ON pca.person_id = pra.person_id
),

copdlat_dat_calculations AS (
    -- COPDLAT_DAT: earliest eligible disorder or administrative evidence after resolution.
    SELECT
        qfc.person_id,
        MIN(ece.evidence_date) AS copdlat_dat
    FROM qof_field_calculations AS qfc
    INNER JOIN eligible_copd_evidence AS ece
        ON
            qfc.person_id = ece.person_id
            AND qfc.copdres_dat < ece.evidence_date
    WHERE qfc.copdres_dat IS NOT NULL
    GROUP BY qfc.person_id
),

qof_field_calculations_extended AS (
    SELECT
        qfc.*,
        cdc.copdlat_dat,
        CASE
            WHEN qfc.copdres_dat IS NULL AND qfc.copdear_dat IS NOT NULL
                THEN qfc.copdear_dat
            WHEN qfc.copdres_dat IS NOT NULL AND qfc.copdear_dat IS NOT NULL
                THEN cdc.copdlat_dat
        END AS eunrescopd_dat
    FROM qof_field_calculations AS qfc
    LEFT JOIN copdlat_dat_calculations AS cdc
        ON qfc.person_id = cdc.person_id
),

spirometry_tests AS (
    -- All spirometry tests with proper QOF field mapping
    SELECT
        person_id,
        clinical_effective_date AS spirometry_date,
        fev1_fvc_ratio,
        is_below_0_7,
        is_valid_spirometry,
        spirometry_interpretation,
        source_cluster_id,
        -- Map to QOF fields
        CASE
            WHEN source_cluster_id = 'FEV1FVC_COD' AND is_below_0_7 = TRUE
                THEN clinical_effective_date
        END AS fev1fvc_below_0_7_date,
        CASE
            WHEN source_cluster_id = 'FEV1FVCL70_COD'
                THEN clinical_effective_date
        END AS fev1fvcl70_date
    FROM {{ ref('int_spirometry_all') }}
    WHERE is_valid_spirometry = TRUE
),

-- QOF Rule Implementation (Exact Specification)
qof_rule_1_pre_april_2023 AS (
    -- RULE 1: If EUNRESCOPD_DAT < 01/04/2023 → SELECT
    SELECT
        qfce.person_id,
        qfce.eunrescopd_dat AS diagnosis_date,
        'Rule 1: Pre-April 2023' AS qof_rule_applied,
        TRUE AS qualifies_for_register,
        'EUNRESCOPD_DAT < 01/04/2023 - automatic inclusion'
            AS qualification_reason,
        NULL AS relevant_spirometry_date,
        NULL AS relevant_spirometry_ratio
    FROM qof_field_calculations_extended AS qfce
    WHERE
        qfce.eunrescopd_dat IS NOT NULL
        AND qfce.eunrescopd_dat < '2023-04-01'
),

patients_for_rule_2_3_4 AS (
    -- Patients with EUNRESCOPD_DAT >= 01/04/2023 (for Rules 2, 3, 4)
    SELECT qfce.*
    FROM qof_field_calculations_extended AS qfce
    WHERE
        qfce.eunrescopd_dat IS NOT NULL
        AND qfce.eunrescopd_dat >= '2023-04-01'
        AND qfce.person_id NOT IN (
            SELECT person_id FROM qof_rule_1_pre_april_2023
        )
),

qof_rule_2_spirometry_timeframe AS (
    -- RULE 2: Spirometry within exact QOF timeframes
    -- Field 24: FEV1FVCDIAG_DAT >= (EUNRESCOPD_DAT – 93 days) AND <= (EUNRESCOPD_DAT + 186 days)
    -- Field 25: FEV1FVCL70DIAG_DAT >= (EUNRESCOPD_DAT – 93 days) AND <= (EUNRESCOPD_DAT + 186 days)
    SELECT DISTINCT
        pfr.person_id,
        pfr.eunrescopd_dat AS diagnosis_date,
        'Rule 2: Post-April 2023 + Spirometry' AS qof_rule_applied,
        TRUE AS qualifies_for_register,
        'Spirometry <0.7 within 93 days before to 186 days after EUNRESCOPD_DAT'
            AS qualification_reason,
        s.spirometry_date AS relevant_spirometry_date,
        s.fev1_fvc_ratio AS relevant_spirometry_ratio
    FROM patients_for_rule_2_3_4 AS pfr
    INNER JOIN spirometry_tests AS s
        ON
            pfr.person_id = s.person_id
            AND s.is_below_0_7 = TRUE  -- FEV1/FVC <0.7
            AND s.spirometry_date >= DATEADD('day', -93, pfr.eunrescopd_dat)   -- 93 days before
            AND s.spirometry_date <= DATEADD('day', 186, pfr.eunrescopd_dat)   -- 186 days after
),

-- RULE 3: Newly registered patients (last 12 months) with spirometry within -93 to +186 days of registration
newly_registered_patients AS (
    SELECT
        person_id,
        registration_start_date AS reg_dat
    FROM {{ ref('dim_person_historical_practice') }}
    WHERE is_current_registration = TRUE
      AND registration_start_date > CURRENT_DATE() - INTERVAL '12 months'
      AND registration_start_date <= CURRENT_DATE()
),

qof_rule_3_newly_registered AS (
    SELECT DISTINCT
        pfr.person_id,
        pfr.eunrescopd_dat AS diagnosis_date,
        'Rule 3: Newly Registered + Spirometry' AS qof_rule_applied,
        TRUE AS qualifies_for_register,
        'Newly registered patient with spirometry <0.7 within 93 days before to 186 days after registration'
            AS qualification_reason,
        s.spirometry_date AS relevant_spirometry_date,
        s.fev1_fvc_ratio AS relevant_spirometry_ratio
    FROM patients_for_rule_2_3_4 AS pfr
    INNER JOIN newly_registered_patients AS nrp
        ON pfr.person_id = nrp.person_id
    INNER JOIN spirometry_tests AS s
        ON
            pfr.person_id = s.person_id
            AND s.is_below_0_7 = TRUE
            AND s.spirometry_date >= DATEADD('day', -93, nrp.reg_dat)
            AND s.spirometry_date <= DATEADD('day', 186, nrp.reg_dat)
    WHERE
        pfr.person_id NOT IN (
            SELECT person_id FROM qof_rule_2_spirometry_timeframe
        )
),

-- RULE 4: All remaining post-April 2023 patients
qof_rule_4_post_april_2023_remaining AS (
    SELECT DISTINCT
        pfr.person_id,
        pfr.eunrescopd_dat AS diagnosis_date,
        'Rule 4: Post-April 2023 (All Remaining)' AS qof_rule_applied,
        TRUE AS qualifies_for_register,
        'EUNRESCOPD_DAT >= 01/04/2023 - included per QOF v51 Rule 4'
            AS qualification_reason,
        NULL AS relevant_spirometry_date,
        NULL AS relevant_spirometry_ratio
    FROM patients_for_rule_2_3_4 AS pfr
    WHERE
        pfr.person_id NOT IN (SELECT person_id FROM qof_rule_2_spirometry_timeframe)
        AND pfr.person_id NOT IN (SELECT person_id FROM qof_rule_3_newly_registered)
),

-- Combine all qualifying patients (deduplicated to one row per person)
all_qualifying_patients_raw AS (
    SELECT * FROM qof_rule_1_pre_april_2023
    UNION ALL
    SELECT * FROM qof_rule_2_spirometry_timeframe
    UNION ALL
    SELECT * FROM qof_rule_3_newly_registered
    UNION ALL
    SELECT * FROM qof_rule_4_post_april_2023_remaining
),

all_qualifying_patients AS (
    SELECT *
    FROM all_qualifying_patients_raw
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id
        ORDER BY
            -- Prioritize by rule number (Rule 1 > Rule 2 > Rule 3 > Rule 4)
            CASE qof_rule_applied
                WHEN 'Rule 1: Pre-April 2023' THEN 1
                WHEN 'Rule 2: Post-April 2023 + Spirometry' THEN 2
                WHEN 'Rule 3: Newly Registered + Spirometry' THEN 3
                WHEN 'Rule 4: Post-April 2023 (All Remaining)' THEN 4
                ELSE 5
            END,
            -- Then by earliest spirometry date for tie-breaking
            relevant_spirometry_date NULLS LAST
    ) = 1
),

-- Get latest spirometry for reporting
latest_spirometry_all AS (
    SELECT
        person_id,
        MAX(spirometry_date) AS latest_spirometry_date,
        COUNT(*) AS total_spirometry_tests
    FROM spirometry_tests
    GROUP BY person_id
),

latest_spirometry_values AS (
    SELECT
        st.person_id,
        st.fev1_fvc_ratio AS latest_spirometry_ratio,
        st.is_below_0_7 AS latest_spirometry_below_0_7
    FROM spirometry_tests AS st
    INNER JOIN latest_spirometry_all AS lsa
        ON
            st.person_id = lsa.person_id
            AND st.spirometry_date = lsa.latest_spirometry_date
    QUALIFY
        ROW_NUMBER()
            OVER (PARTITION BY st.person_id ORDER BY st.spirometry_date DESC)
        = 1
),

-- Keep unable spirometry data for analytics (not required for register inclusion)
unable_spirometry_summary AS (
    SELECT
        person_id,
        MAX(clinical_effective_date) AS latest_unable_spirometry_date,
        COUNT(*) AS total_unable_spirometry_records
    FROM {{ ref('int_unable_spirometry_all') }}
    GROUP BY person_id
)

-- Final output with exact QOF field mapping
SELECT
    qfce.person_id,
    qfce.current_age AS age,

    -- Core register status
    qfce.eunrescopd_dat AS earliest_unresolved_diagnosis_date,
    qfce.copdear_dat AS earliest_diagnosis_date,
    qfce.latest_evidence_date AS latest_diagnosis_date,

    -- QOF v51 fields and retained descriptive aliases
    qfce.copdres_dat AS latest_resolved_date,
    CASE
        WHEN qfce.copdres_dat > qfce.copdear_dat THEN qfce.copdres_dat
    END AS latest_resolved_after_earliest_date,
    qfce.copdlat_dat AS earliest_diagnosis_after_latest_resolved,
    qfce.copd_diagnosis_count,
    qfce.copd_disorder_code_count,
    qfce.copd_admin_code_count,
    aqp.relevant_spirometry_date AS qof_relevant_spirometry_date,
    aqp.relevant_spirometry_ratio AS qof_relevant_spirometry_ratio,
    lsa.latest_spirometry_date,

    -- Spirometry data
    lsv.latest_spirometry_ratio,
    uss.latest_unable_spirometry_date,

    -- Concept arrays
    qfce.all_copd_concept_codes,
    qfce.all_copd_concept_displays,

    -- Register status
    COALESCE(aqp.qualifies_for_register, FALSE) AS is_on_register,
    COALESCE(aqp.qof_rule_applied, 'Not Qualified') AS qof_rule_applied,
    COALESCE(aqp.qualification_reason, 'No COPD diagnosis or resolved') AS qualification_reason,

    -- Temporal flags
    COALESCE(qfce.eunrescopd_dat < '2023-04-01', FALSE) AS is_pre_april_2023_diagnosis,
    COALESCE(qfce.eunrescopd_dat >= '2023-04-01', FALSE) AS is_post_april_2023_diagnosis,

    -- Spirometry confirmation flags
    COALESCE(lsv.latest_spirometry_below_0_7, FALSE) AS latest_spirometry_confirms_copd,

    -- Analytics counts
    COALESCE(lsa.total_spirometry_tests, 0) AS total_spirometry_tests,
    COALESCE(uss.total_unable_spirometry_records, 0) AS total_unable_spirometry_records,

    -- Rule qualification flags
    COALESCE(aqp.qof_rule_applied = 'Rule 1: Pre-April 2023', FALSE) AS qualified_rule_1,
    COALESCE(aqp.qof_rule_applied = 'Rule 2: Post-April 2023 + Spirometry', FALSE) AS qualified_rule_2,
    COALESCE(aqp.qof_rule_applied = 'Rule 3: Newly Registered + Spirometry', FALSE) AS qualified_rule_3,
    COALESCE(aqp.qof_rule_applied = 'Rule 4: Post-April 2023 (All Remaining)', FALSE) AS qualified_rule_4,

    -- Flag for patients who have "unable to spirometry" codes (for analytics, not register requirement)
    COALESCE(uss.total_unable_spirometry_records > 0, FALSE) AS has_unable_spirometry_code

FROM qof_field_calculations_extended AS qfce
LEFT JOIN all_qualifying_patients AS aqp
    ON qfce.person_id = aqp.person_id
LEFT JOIN latest_spirometry_all AS lsa
    ON qfce.person_id = lsa.person_id
LEFT JOIN latest_spirometry_values AS lsv
    ON qfce.person_id = lsv.person_id
LEFT JOIN unable_spirometry_summary AS uss
    ON qfce.person_id = uss.person_id
WHERE COALESCE(aqp.qualifies_for_register, FALSE) = TRUE

ORDER BY person_id
