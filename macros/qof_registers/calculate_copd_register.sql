{% macro calculate_copd_register(reference_date_expr='CURRENT_DATE()') %}
    {# Pair: fct_person_copd_register.sql. This macro is strict as-of and derives age at the reference date where used; the live fact includes future-dated records. #}
    {#
    Calculates COPD register status at a given reference date.

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
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, register_name, is_on_register
    #}

    WITH copd_diagnoses_filtered AS (
        SELECT
            person_id,
            clinical_effective_date,
            is_disorder_code,
            is_admin_code,
            is_resolved_code
        FROM {{ ref('int_copd_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }} AND (date_recorded IS NULL OR CAST(date_recorded AS DATE) <= {{ reference_date_expr }})
    ),

    copd_person_aggregates AS (
        SELECT
            person_id,
            MIN(
                CASE
                    WHEN is_disorder_code
                        OR (
                            is_admin_code
                            AND clinical_effective_date > DATEADD('year', -2, {{ reference_date_expr }})
                        )
                        THEN clinical_effective_date
                END
            ) AS copdear_dat,
            MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) AS copdres_dat
        FROM copd_diagnoses_filtered
        GROUP BY person_id
    ),

    copdlat_dat_calc AS (
        SELECT
            agg.person_id,
            MIN(df.clinical_effective_date) AS copdlat_dat
        FROM copd_person_aggregates agg
        INNER JOIN copd_diagnoses_filtered df
            ON agg.person_id = df.person_id
            AND (
                df.is_disorder_code
                OR (
                    df.is_admin_code
                    AND df.clinical_effective_date > DATEADD('year', -2, {{ reference_date_expr }})
                )
            )
            AND agg.copdres_dat < df.clinical_effective_date
        WHERE agg.copdres_dat IS NOT NULL
        GROUP BY agg.person_id
    ),

    eunrescopd_dat_calc AS (
        SELECT
            agg.person_id,
            agg.copdear_dat,
            agg.copdres_dat,
            cdc.copdlat_dat,
            CASE
                WHEN agg.copdres_dat IS NULL AND agg.copdear_dat IS NOT NULL
                    THEN agg.copdear_dat
                WHEN agg.copdres_dat IS NOT NULL AND agg.copdear_dat IS NOT NULL
                    THEN cdc.copdlat_dat
            END AS eunrescopd_dat
        FROM copd_person_aggregates agg
        LEFT JOIN copdlat_dat_calc cdc ON agg.person_id = cdc.person_id
    ),

    spirometry_filtered AS (
        SELECT
            person_id,
            clinical_effective_date AS spirometry_date,
            fev1_fvc_ratio,
            is_below_0_7,
            is_valid_spirometry
        FROM {{ ref('int_spirometry_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }} AND (date_recorded IS NULL OR CAST(date_recorded AS DATE) <= {{ reference_date_expr }})
          AND is_valid_spirometry = TRUE
          AND is_below_0_7 = TRUE
    ),

    -- Rule 1: Pre-April 2023 automatic inclusion
    rule_1_qualifiers AS (
        SELECT
            person_id,
            eunrescopd_dat,
            1 AS rule_number
        FROM eunrescopd_dat_calc
        WHERE eunrescopd_dat IS NOT NULL
          AND eunrescopd_dat < '2023-04-01'
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
            pap.person_id,
            pap.eunrescopd_dat,
            2 AS rule_number
        FROM post_april_patients pap
        INNER JOIN spirometry_filtered sf
            ON pap.person_id = sf.person_id
            AND sf.spirometry_date >= DATEADD('day', -93, pap.eunrescopd_dat)
            AND sf.spirometry_date <= DATEADD('day', 186, pap.eunrescopd_dat)
    ),

    -- Rule 3: Newly registered patients (last 12 months) with spirometry within -93 to +186 days of registration
    newly_registered_patients AS (
        SELECT
            person_id,
            registration_start_date AS reg_dat
        FROM {{ ref('dim_person_historical_practice') }}
        -- Registered as of the reference date (point-in-time), NOT is_current_registration
        -- which reflects status today. Mirror the QOF GMS rule: registration started in the
        -- 12 months up to the reference date and not ended (death-adjusted) by then.
        WHERE registration_start_date > {{ reference_date_expr }} - INTERVAL '12 months'
          AND registration_start_date <= {{ reference_date_expr }}
          AND (effective_end_date IS NULL OR effective_end_date > {{ reference_date_expr }})
    ),

    rule_3_qualifiers AS (
        SELECT DISTINCT
            pap.person_id,
            pap.eunrescopd_dat,
            3 AS rule_number
        FROM post_april_patients pap
        INNER JOIN newly_registered_patients nrp
            ON pap.person_id = nrp.person_id
        INNER JOIN spirometry_filtered sf
            ON pap.person_id = sf.person_id
            AND sf.spirometry_date >= DATEADD('day', -93, nrp.reg_dat)
            AND sf.spirometry_date <= DATEADD('day', 186, nrp.reg_dat)
        WHERE pap.person_id NOT IN (SELECT person_id FROM rule_2_qualifiers)
    ),

    -- Rule 4: All remaining post-April 2023 patients
    -- The spec's Rule 4 says: "If EUNRESCOPD_DAT >= 01/04/2023 → Select"
    -- This includes ALL remaining patients - no "unable to spirometry" code required
    rule_4_qualifiers AS (
        SELECT DISTINCT
            pap.person_id,
            pap.eunrescopd_dat,
            4 AS rule_number
        FROM post_april_patients pap
        WHERE pap.person_id NOT IN (SELECT person_id FROM rule_2_qualifiers)
          AND pap.person_id NOT IN (SELECT person_id FROM rule_3_qualifiers)
    ),

    all_qualifiers AS (
        SELECT person_id, eunrescopd_dat, rule_number FROM rule_1_qualifiers
        UNION ALL
        SELECT person_id, eunrescopd_dat, rule_number FROM rule_2_qualifiers
        UNION ALL
        SELECT person_id, eunrescopd_dat, rule_number FROM rule_3_qualifiers
        UNION ALL
        SELECT person_id, eunrescopd_dat, rule_number FROM rule_4_qualifiers
    ),

    copd_register_logic AS (
        SELECT
            aq.person_id,
            'COPD' AS register_name,
            TRUE AS is_on_register
        FROM (
            SELECT person_id FROM all_qualifiers GROUP BY person_id
        ) aq
    )

    SELECT
        person_id,
        register_name,
        is_on_register
    FROM copd_register_logic

{% endmacro %}
