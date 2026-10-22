-- LTC LCS case-finding exclusions, one row per person with an excluding condition.
-- ICS_METABOLIC_LTC: nine LTC LCS registers OR any hyperlipidaemia record ever.
-- ICS_RESP_LTC: LTC LCS COPD, adult asthma and CYP asthma registers.
-- Programme COPD and NAFLD register models implement the exported EMIS searches;
-- their aggregate counts differ from EMIS. Other conditions use QOF or clinical
-- register proxies for unexported library items. The QOF CKD register is known to
-- differ materially from the EMIS LTC LCS CKD register. Adult asthma requires age >= 18;
-- CYP asthma has its own register.

WITH ltc_summary_conditions AS (
    SELECT
        person_id,
        condition_code,
        earliest_diagnosis_date

    FROM {{ ref('fct_person_ltc_summary') }}
    WHERE condition_code IN (
        'CKD',
        'AF',
        'HTN',
        'CHD',
        'STIA',
        'PAD',
        'HF',
        'AST',
        'CYP_AST'
    )
    AND is_on_register = TRUE
    AND (
        condition_code != 'AST'
        OR person_id IN (
            SELECT person_id FROM {{ ref('dim_person_age') }} WHERE age >= 18
        )
    )
),

diabetes_all AS (
    -- EMIS Diabetes Register: age >= 17, DM_COD with no later resolved record.
    SELECT
        person_id,
        MIN(earliest_diagnosis_date) AS earliest_diagnosis_date
    FROM {{ ref('fct_person_diabetes_register') }}
    WHERE is_on_register = TRUE
    GROUP BY person_id
),

programme_registers AS (
    SELECT person_id, 'COPD' AS condition_code
    FROM {{ ref('int_ltc_lcs_copd_register') }}
    UNION ALL
    SELECT person_id, 'NAF' AS condition_code
    FROM {{ ref('int_ltc_lcs_nafld_register') }}
),

nafld_exception_roots AS (
    SELECT translated_to_snomed_code AS concept_code
    FROM {{ ref('stg_reference_ltc_lcs_exceptions') }}
    WHERE valueset_id = '08654d02-552a-7784-076c-e7fcc941d284'
),

nafld_exceptions AS (
    SELECT concept_code FROM nafld_exception_roots
    UNION
    SELECT tc.sub_type_id::varchar AS concept_code
    FROM {{ ref('stg_nhsd_snomed_sct_transitive_closure') }} AS tc
    INNER JOIN nafld_exception_roots AS ex
        ON tc.super_type_id::varchar = ex.concept_code
),

programme_diagnosis_dates AS (
    SELECT
        person_id,
        'COPD' AS condition_code,
        MIN(clinical_effective_date) AS earliest_diagnosis_date
    FROM ({{ get_observations("'COPD_COD'") }})
    GROUP BY person_id, condition_code
    UNION ALL
    SELECT obs.person_id, 'NAF' AS condition_code,
        MIN(obs.clinical_effective_date) AS earliest_diagnosis_date
    FROM ({{ get_ltc_lcs_observations("08654d02-552a-7784-076c-e7fcc941d284,f0bfa8e2-972c-1cba-326f-573ef42e6124") }}) AS obs
    WHERE NOT EXISTS (
        SELECT 1 FROM nafld_exceptions AS ex
        WHERE ex.concept_code = obs.concept_code
    )
    GROUP BY obs.person_id
),

hyperlipidaemia AS (
    -- XML 9a9685d5: description 293299018, Hyperlipidaemia, with descendants.
    -- Warehouse value set matched by original code and include_children, not XML GUID.
    SELECT person_id, MIN(clinical_effective_date) AS earliest_diagnosis_date
    FROM ({{ get_ltc_lcs_observations("3b4d5851-8dbb-2c24-7899-5ac257a8e478") }})
    GROUP BY person_id
),

all_conditions AS (
    SELECT * FROM ltc_summary_conditions
    UNION ALL
    SELECT
        person_id,
        'DM' AS condition_code,
        earliest_diagnosis_date
    FROM diabetes_all
    UNION ALL
    SELECT r.person_id, r.condition_code, d.earliest_diagnosis_date
    FROM programme_registers AS r
    LEFT JOIN programme_diagnosis_dates AS d
        ON r.person_id = d.person_id AND r.condition_code = d.condition_code
    UNION ALL
    SELECT person_id, 'HYPERLIPIDAEMIA' AS condition_code, earliest_diagnosis_date
    FROM hyperlipidaemia
),

person_level_aggregation AS (
    SELECT
        person_id,
        MIN(earliest_diagnosis_date) AS earliest_excluding_condition_date,
        BOOLOR_AGG(condition_code = 'CKD') AS has_ckd,
        BOOLOR_AGG(condition_code = 'AF') AS has_af,
        BOOLOR_AGG(condition_code = 'COPD') AS has_copd,
        BOOLOR_AGG(condition_code = 'HTN') AS has_hypertension,
        BOOLOR_AGG(condition_code = 'CHD') AS has_chd,
        BOOLOR_AGG(condition_code = 'STIA') AS has_stia,
        BOOLOR_AGG(condition_code = 'PAD') AS has_pad,
        BOOLOR_AGG(condition_code = 'HF') AS has_hf,
        BOOLOR_AGG(condition_code = 'DM') AS has_diabetes,
        BOOLOR_AGG(condition_code = 'HYPERLIPIDAEMIA') AS has_hyperlipidaemia,
        BOOLOR_AGG(condition_code = 'NAF') AS has_nafld,
        BOOLOR_AGG(condition_code = 'AST') AS has_asthma,
        BOOLOR_AGG(condition_code = 'CYP_AST') AS has_cyp_asthma
    FROM all_conditions
    GROUP BY person_id
)

SELECT
    person_id,
    has_ckd,
    has_af,
    has_copd,
    has_hypertension,
    has_chd,
    has_stia,
    has_pad,
    has_hf,
    has_diabetes,
    has_hyperlipidaemia,
    has_nafld,
    has_asthma,
    has_cyp_asthma,
    earliest_excluding_condition_date,

    -- ICS_METABOLIC_LTC: register membership or hyperlipidaemia ever
    -- Used by DM, CKD, CVD, HTN, AF, HF case finding base populations
    (
        has_ckd OR has_af OR has_hypertension OR has_chd
        OR has_stia OR has_pad OR has_hf OR has_diabetes
        OR has_hyperlipidaemia OR has_nafld
    ) AS has_metabolic_excluding_condition,

    -- ICS_RESP_LTC equivalent: respiratory conditions only
    -- Used by CYP Asthma case finding base population
    (has_copd OR has_asthma OR has_cyp_asthma) AS has_respiratory_excluding_condition,

    -- Combined flag retained for existing consumers.
    (
        has_ckd OR has_af OR has_copd OR has_hypertension OR has_chd
        OR has_stia OR has_pad OR has_hf OR has_diabetes
        OR has_hyperlipidaemia OR has_nafld OR has_asthma OR has_cyp_asthma
    ) AS has_excluding_condition
FROM person_level_aggregation
