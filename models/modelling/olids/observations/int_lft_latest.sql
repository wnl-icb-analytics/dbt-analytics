{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Composite liver function test status per person.
Joins the latest ALT, GGT, and bilirubin observations per person (each sourced from its own
int_<component>_latest model) and derives a single high_lft flag that is TRUE when any
component exceeds its QAdmissions abnormal-LFT threshold, approximately three times the clinical ULN.

Thresholds are sourced from the qadmissions_lab_thresholds seed (direction = 'high').
*/

WITH thresholds AS (
    SELECT
        MAX(CASE WHEN measurement = 'alt' THEN threshold END) AS alt_uln,
        MAX(CASE WHEN measurement = 'ggt' THEN threshold END) AS ggt_uln,
        MAX(CASE WHEN measurement = 'bilirubin' THEN threshold END) AS bilirubin_uln
    FROM {{ ref('qadmissions_lab_thresholds') }}
    WHERE direction = 'high'
),

alt AS (
    SELECT
        person_id,
        inferred_value AS alt_value,
        clinical_effective_date AS alt_date
    FROM {{ ref('int_alt_latest') }}
),

ggt AS (
    SELECT
        person_id,
        inferred_value AS ggt_value,
        clinical_effective_date AS ggt_date
    FROM {{ ref('int_ggt_latest') }}
),

bilirubin AS (
    SELECT
        person_id,
        inferred_value AS bilirubin_value,
        clinical_effective_date AS bilirubin_date
    FROM {{ ref('int_bilirubin_latest') }}
),

joined AS (
    SELECT
        COALESCE(alt.person_id, ggt.person_id, bilirubin.person_id) AS person_id,
        alt.alt_value,
        alt.alt_date,
        ggt.ggt_value,
        ggt.ggt_date,
        bilirubin.bilirubin_value,
        bilirubin.bilirubin_date
    FROM alt
    FULL OUTER JOIN ggt
        ON alt.person_id = ggt.person_id
    FULL OUTER JOIN bilirubin
        ON COALESCE(alt.person_id, ggt.person_id) = bilirubin.person_id
)

SELECT
    j.person_id,
    j.alt_value,
    j.alt_date,
    j.ggt_value,
    j.ggt_date,
    j.bilirubin_value,
    j.bilirubin_date,
    j.alt_value > t.alt_uln AS is_high_alt,
    j.ggt_value > t.ggt_uln AS is_high_ggt,
    j.bilirubin_value > t.bilirubin_uln AS is_high_bilirubin,
    COALESCE(j.alt_value > t.alt_uln, FALSE)
        OR COALESCE(j.ggt_value > t.ggt_uln, FALSE)
        OR COALESCE(j.bilirubin_value > t.bilirubin_uln, FALSE) AS high_lft,
    GREATEST_IGNORE_NULLS(j.alt_date, j.ggt_date, j.bilirubin_date) AS last_lft_date 
FROM joined j
CROSS JOIN thresholds t
