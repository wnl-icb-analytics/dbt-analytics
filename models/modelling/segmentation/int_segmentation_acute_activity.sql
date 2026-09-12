{{
    config(
        materialized='table',
        cluster_by=['sk_patient_id'])
}}

-- Acute activity block for segmentation. Grain: one row per sk_patient_id
-- with any ECDS attendance or non-elective admission in the 12 months ending
-- on the segmentation reporting date; sk_patient_id '1' is a shared junk key
-- and is excluded.
--
-- ed_attendances_12mo counts attendances in every ECDS urgent and emergency
-- care setting (Type 1/2 A&E, UTC, WiC, SDEC) by arrival date.
-- nel_admissions_12mo counts spells with an emergency admission method (2x)
-- by admission date, excluding 2C (baby born at home).

WITH ed AS (
    SELECT
        sk_patient_id,
        COUNT(DISTINCT visit_occurrence_id) AS ed_attendances_12mo
    FROM {{ ref('int_sus_uec_encounter') }}
    WHERE
        start_date BETWEEN DATEADD('month', -12, {{ segmentation_reporting_date() }})
        AND {{ segmentation_reporting_date() }}
        AND sk_patient_id IS NOT NULL
        AND sk_patient_id != '1'
    GROUP BY sk_patient_id
),

nel AS (
    SELECT
        sk_patient_id,
        COUNT(DISTINCT visit_occurrence_id) AS nel_admissions_12mo
    FROM {{ ref('int_sus_apc_imputed_spells') }}
    WHERE
        LEFT(spell_admission_method, 1) = '2'
        AND spell_admission_method != '2C'
        AND start_date BETWEEN DATEADD('month', -12, {{ segmentation_reporting_date() }})
        AND {{ segmentation_reporting_date() }}
        AND sk_patient_id IS NOT NULL
        AND sk_patient_id != '1'
    GROUP BY sk_patient_id
)

SELECT
    COALESCE(e.sk_patient_id, n.sk_patient_id) AS sk_patient_id,
    ZEROIFNULL(e.ed_attendances_12mo) AS ed_attendances_12mo,
    ZEROIFNULL(n.nel_admissions_12mo) AS nel_admissions_12mo
FROM ed AS e
FULL OUTER JOIN nel AS n
    ON e.sk_patient_id = n.sk_patient_id
