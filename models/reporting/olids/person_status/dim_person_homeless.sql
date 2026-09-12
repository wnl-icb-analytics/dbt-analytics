{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'care_home', 'residence'],
        cluster_by=['person_id'])
}}

-- Homeless persons. Grain: one row per person whose latest residential
-- status code (RESIDE_COD or HOMELESS_COD) is a homelessness code, or who is
-- currently registered with the Camden Health Improvement Practice (Y02674).
--
-- HOMELESS_COD codes are also in RESIDE_COD. When a homelessness code and
-- another residential code share the latest date, homelessness wins, as in
-- the UKHSA rule HOMELESS_DAT >= RESIDE_DAT. Remaining same-day ties break
-- on concept code so the result is stable between builds.

WITH homeless_codes AS (
    SELECT DISTINCT
        cluster_id,
        code
    FROM {{ ref('stg_reference_combined_codesets') }}
    WHERE cluster_id IN ('RESIDE_COD', 'HOMELESS_COD')
),

residential_records AS (
    SELECT
        obs.person_id,
        DATE(obs.clinical_effective_date) AS residential_date,
        obs.mapped_concept_code AS concept_code,
        MIN(obs.mapped_concept_display) AS code_description,
        BOOLOR_AGG(cc.cluster_id = 'HOMELESS_COD') AS is_homeless_code
    FROM {{ ref('stg_olids_observation') }} AS obs
    INNER JOIN homeless_codes AS cc
        ON obs.mapped_concept_code = cc.code
    WHERE
        obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= CURRENT_DATE
    GROUP BY
        obs.person_id,
        DATE(obs.clinical_effective_date),
        obs.mapped_concept_code
),

latest_residential AS (
    SELECT *
    FROM residential_records
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id
        ORDER BY residential_date DESC, is_homeless_code DESC, concept_code
    ) = 1
),

currently_homeless AS (
    SELECT
        person_id,
        residential_date AS latest_residential_date,
        concept_code,
        code_description
    FROM latest_residential
    WHERE is_homeless_code
),

registered_chip AS (
    SELECT
        person_id,
        TRUE AS registered_chip
    FROM {{ ref('dim_person_demographics') }}
    WHERE is_active AND NOT is_deceased AND practice_code = 'Y02674'
)

SELECT
    COALESCE(hom.person_id, reg.person_id) AS person_id,
    hom.latest_residential_date,
    hom.concept_code,
    hom.code_description,
    reg.registered_chip
FROM currently_homeless AS hom
FULL OUTER JOIN registered_chip AS reg
    ON hom.person_id = reg.person_id
