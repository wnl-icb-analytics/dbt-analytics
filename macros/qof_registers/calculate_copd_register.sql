{% macro calculate_copd_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_copd_register.sql. This macro is strict as-of and derives age at the reference date where used; the live fact includes future-dated records. #}
    {#
    Calculates COPD register status at one or more reference dates.

    Implements QOF v51 COPD Rules 1-4:
    - Rule 1: EUNRESCOPD_DAT < 01/04/2023 → automatic inclusion
    - Rule 2: EUNRESCOPD_DAT >= 01/04/2023 + spirometry <0.7 within -93 to +186 days of diagnosis
    - Rule 3: EUNRESCOPD_DAT >= 01/04/2023 + newly registered (last 12 months) + spirometry <0.7 within -93 to +186 days of registration
    - Rule 4: EUNRESCOPD_DAT >= 01/04/2023 → all remaining patients included

    QOF v51 diagnosis derivation:
    - COPDDIAG_COD disorder evidence counts at any date up to the reference date.
    - COPDPROC_COD administrative evidence counts only in the preceding two years.
    - COPDEAR_DAT is the earliest eligible disorder or administrative evidence.
    - COPDRES_DAT is the latest resolved code up to the reference date.
    - COPDLAT_DAT is the earliest eligible evidence after COPDRES_DAT.
    - EUNRESCOPD_DAT is COPDEAR_DAT when never resolved, otherwise COPDLAT_DAT.

    Note: Rule 4 (EUNRESCOPD_DAT >= 01/04/2023 → Select) is the spec's catch-all and makes
    Rules 2-3 non-gating for the register. The spirometry
    rules populate FEV1FVCDIAG/REG dates used by downstream indicators, not the register.

    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person with eligible COPD evidence known by each reference date:
        reference_date, person_id, register_name, is_on_register,
        earliest_diagnosis_date (COPDEAR_DAT), latest_diagnosis_date (latest eligible evidence),
        latest_resolved_date (COPDRES_DAT), earliest_unresolved_diagnosis_date (EUNRESCOPD_DAT),
        qof_rule_number (1-4, null when not on the register)
    #}

    WITH reference_dates AS (
        {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
    ),

    copd_diagnoses_filtered AS (
        SELECT
            ref_date.reference_date,
            diag.person_id,
            diag.clinical_effective_date,
            diag.is_disorder_code,
            diag.is_admin_code,
            diag.is_resolved_code,
            -- Disorder codes count at any date; administrative codes only in the two years before the reference date.
            diag.is_disorder_code
                OR (
                    diag.is_admin_code
                    AND diag.clinical_effective_date > DATEADD('year', -2, ref_date.reference_date)
                ) AS is_eligible_evidence
        FROM {{ ref('int_copd_diagnoses_all') }} AS diag
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('diag.clinical_effective_date', 'diag.date_recorded', 'ref_date.reference_date') }}
    ),

    copd_person_aggregates AS (
        SELECT
            reference_date,
            person_id,
            MIN(CASE WHEN is_eligible_evidence THEN clinical_effective_date END) AS copdear_dat,
            MAX(CASE WHEN is_eligible_evidence THEN clinical_effective_date END) AS latest_evidence_date,
            MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) AS copdres_dat
        FROM copd_diagnoses_filtered
        GROUP BY reference_date, person_id
    ),

    copdlat_dat_calc AS (
        SELECT
            agg.reference_date,
            agg.person_id,
            MIN(df.clinical_effective_date) AS copdlat_dat
        FROM copd_person_aggregates AS agg
        INNER JOIN copd_diagnoses_filtered AS df
            ON agg.person_id = df.person_id
            AND agg.reference_date = df.reference_date
            AND df.is_eligible_evidence
            AND agg.copdres_dat < df.clinical_effective_date
        WHERE agg.copdres_dat IS NOT NULL
        GROUP BY agg.reference_date, agg.person_id
    ),

    eunrescopd_dat_calc AS (
        SELECT
            agg.reference_date,
            agg.person_id,
            agg.copdear_dat,
            agg.latest_evidence_date,
            agg.copdres_dat,
            cdc.copdlat_dat,
            CASE
                WHEN agg.copdres_dat IS NULL AND agg.copdear_dat IS NOT NULL
                    THEN agg.copdear_dat
                WHEN agg.copdres_dat IS NOT NULL AND agg.copdear_dat IS NOT NULL
                    THEN cdc.copdlat_dat
            END AS eunrescopd_dat
        FROM copd_person_aggregates AS agg
        LEFT JOIN copdlat_dat_calc AS cdc
            ON agg.person_id = cdc.person_id
            AND agg.reference_date = cdc.reference_date
    ),

    spirometry_filtered AS (
        SELECT
            ref_date.reference_date,
            spiro.person_id,
            spiro.clinical_effective_date AS spirometry_date
        FROM {{ ref('int_spirometry_all') }} AS spiro
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('spiro.clinical_effective_date', 'spiro.date_recorded', 'ref_date.reference_date') }}
        WHERE spiro.is_valid_spirometry = TRUE
          AND spiro.is_below_0_7 = TRUE
    ),

    -- Patients for Rules 2-4 (post-April 2023)
    post_april_patients AS (
        SELECT *
        FROM eunrescopd_dat_calc
        WHERE eunrescopd_dat IS NOT NULL
          AND eunrescopd_dat >= '2023-04-01'
    ),

    -- Rule 2: Spirometry within -93 to +186 days of diagnosis
    rule_2_qualifiers AS (
        SELECT DISTINCT
            pap.reference_date,
            pap.person_id
        FROM post_april_patients AS pap
        INNER JOIN spirometry_filtered AS sf
            ON pap.person_id = sf.person_id
            AND pap.reference_date = sf.reference_date
            AND sf.spirometry_date >= DATEADD('day', -93, pap.eunrescopd_dat)
            AND sf.spirometry_date <= DATEADD('day', 186, pap.eunrescopd_dat)
    ),

    -- Rule 3: Newly registered patients (last 12 months) with spirometry within -93 to +186 days of registration
    newly_registered_patients AS (
        SELECT
            ref_date.reference_date,
            reg.person_id,
            reg.registration_start_date AS reg_dat
        FROM {{ ref('dim_person_historical_practice') }} AS reg
        -- Registered as of the reference date (point-in-time), NOT is_current_registration
        -- which reflects status today. Mirror the QOF GMS rule: registration started in the
        -- 12 months up to the reference date and not ended (death-adjusted) by then.
        INNER JOIN reference_dates AS ref_date
            ON reg.registration_start_date > ref_date.reference_date - INTERVAL '12 months'
            AND reg.registration_start_date <= ref_date.reference_date
            AND (reg.effective_end_date IS NULL OR reg.effective_end_date > ref_date.reference_date)
    ),

    rule_3_qualifiers AS (
        SELECT DISTINCT
            pap.reference_date,
            pap.person_id
        FROM post_april_patients AS pap
        INNER JOIN newly_registered_patients AS nrp
            ON pap.person_id = nrp.person_id
            AND pap.reference_date = nrp.reference_date
        INNER JOIN spirometry_filtered AS sf
            ON pap.person_id = sf.person_id
            AND pap.reference_date = sf.reference_date
            AND sf.spirometry_date >= DATEADD('day', -93, nrp.reg_dat)
            AND sf.spirometry_date <= DATEADD('day', 186, nrp.reg_dat)
        LEFT JOIN rule_2_qualifiers AS r2
            ON pap.person_id = r2.person_id
            AND pap.reference_date = r2.reference_date
        WHERE r2.person_id IS NULL
    ),

    copd_register_logic AS (
        SELECT
            calc.reference_date,
            calc.person_id,
            'COPD' AS register_name,
            calc.eunrescopd_dat IS NOT NULL AS is_on_register,
            calc.copdear_dat AS earliest_diagnosis_date,
            calc.latest_evidence_date AS latest_diagnosis_date,
            calc.copdres_dat AS latest_resolved_date,
            calc.eunrescopd_dat AS earliest_unresolved_diagnosis_date,
            CASE
                WHEN calc.eunrescopd_dat IS NULL THEN NULL
                -- Rule 1: Pre-April 2023 automatic inclusion
                WHEN calc.eunrescopd_dat < '2023-04-01' THEN 1
                WHEN r2.person_id IS NOT NULL THEN 2
                WHEN r3.person_id IS NOT NULL THEN 3
                -- Rule 4: all remaining post-April 2023 patients; no "unable to spirometry" code required
                ELSE 4
            END AS qof_rule_number
        FROM eunrescopd_dat_calc AS calc
        LEFT JOIN rule_2_qualifiers AS r2
            ON calc.person_id = r2.person_id
            AND calc.reference_date = r2.reference_date
        LEFT JOIN rule_3_qualifiers AS r3
            ON calc.person_id = r3.person_id
            AND calc.reference_date = r3.reference_date
    )

    SELECT
        reference_date,
        person_id,
        register_name,
        is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        latest_resolved_date,
        earliest_unresolved_diagnosis_date,
        qof_rule_number
    FROM copd_register_logic

{% endmacro %}
