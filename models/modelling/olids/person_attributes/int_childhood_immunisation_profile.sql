{{ config(materialized='table', cluster_by=['person_id']) }}

/*
Per-child immunisation dose counts inside the NICE age windows, one row per
currently registered child under 20 in int_childhood_imms_current_population.
Doses count administration records and vaccine orders by vaccine group and
distinct date. Read the evidence before programme dose assignment, which can
discard valid administrations after declined or contraindicated records.
Contraindication flags come from contraindicated event codes for the group
and are false when nothing is recorded. Age windows include the lower bound
and exclude the upper one.

Groups: DTAP_PRIMARY (4-, 5- or 6-in-1), DTAP_BOOSTER (4-in-1 preschool), MMR (MMR and
MMRV), ROTAVIRUS and MENB. Windows are measured from the approximate birth date.
*/

WITH population AS (
    SELECT person_id, birth_date_approx::DATE AS birth_date_approx
    FROM {{ ref('int_childhood_imms_current_population') }}
),

event_codes AS (
    SELECT DISTINCT
        snomedconceptid::VARCHAR AS code,
        CASE vaccine
            WHEN 'MMR' THEN 'MMR'
            WHEN 'MMRV' THEN 'MMR'
            WHEN 'Rotavirus' THEN 'ROTAVIRUS'
            WHEN 'MenB' THEN 'MENB'
            WHEN 'DTaP/IPV/Hib/HepB/6-in-1' THEN 'DTAP_PRIMARY'
            WHEN 'dTaP/IPV/4-in-1' THEN 'DTAP_PRIMARY'
        END AS vaccine_group,
        CASE
            WHEN proposedcluster ILIKE '%_ADM' THEN 'Administration'
            WHEN proposedcluster ILIKE '%_DRUG' THEN 'Administration_drug'
            WHEN proposedcluster ILIKE '%_CONTRA' THEN 'Contraindicated'
        END AS event_type
    FROM {{ ref('stg_reference_childhood_imms_codes') }}
    WHERE vaccine IN ('MMR', 'MMRV', 'Rotavirus', 'MenB',
        'DTaP/IPV/Hib/HepB/6-in-1', 'dTaP/IPV/4-in-1')
        AND (proposedcluster ILIKE '%_ADM' OR proposedcluster ILIKE '%_DRUG'
            OR proposedcluster ILIKE '%_CONTRA')

    UNION ALL

    -- QOF v51 DTP1/2/3 accepts each DTP-containing formulation.
    SELECT DISTINCT
        code,
        'DTAP_PRIMARY' AS vaccine_group,
        CASE
            WHEN cluster_id = 'DTPCON_COD' THEN 'Contraindicated'
            ELSE 'Administration'
        END AS event_type
    FROM {{ ref('stg_reference_combined_codesets') }}
    WHERE source = 'PCD'
        AND cluster_id IN ('4IN1VAC_COD', '5IN1VAC_COD', '6IN1VAC_COD', 'DTPCON_COD')

    UNION ALL

    SELECT DISTINCT
        code,
        'DTAP_BOOSTER' AS vaccine_group,
        CASE
            WHEN cluster_id = 'DTAPIPVCON_COD' THEN 'Contraindicated'
            ELSE 'Administration'
        END AS event_type
    FROM {{ ref('stg_reference_combined_codesets') }}
    WHERE source = 'PCD'
        AND cluster_id IN ('DTAPIPVVACBOOST_COD', 'DTAPIPVCON_COD')

    UNION ALL

    -- These QOF drug refsets are not included in the combined PCD code extract.
    SELECT DISTINCT
        referenced_component_id AS code,
        'DTAP_PRIMARY' AS vaccine_group,
        'Administration_drug' AS event_type
    FROM {{ ref('stg_nhsd_snomed_sct_refset_simple') }}
    WHERE active
        AND ref_set_id IN (
            '72391000001101', -- 4IN1VACDRUG_COD
            '72381000001103', -- 5IN1VACDRUG_COD
            '72371000001100'  -- 6IN1VACDRUG_COD
        )
),

codes AS (
    -- QOF v51 VI003 requires explicit booster coding. A 4-in-1 product alone
    -- establishes its formulation, not its place in the vaccination course.
    SELECT DISTINCT code, vaccine_group, event_type FROM event_codes
),

events AS (
    SELECT
        obs.person_id,
        obs.clinical_effective_date::DATE AS event_date,
        codes.event_type,
        codes.vaccine_group
    FROM {{ ref('stg_olids_observation') }} AS obs
    INNER JOIN codes ON obs.mapped_concept_code = codes.code
    INNER JOIN population AS p ON obs.person_id = p.person_id
    WHERE obs.clinical_effective_date::DATE BETWEEN DATE_TRUNC('month', p.birth_date_approx) AND CURRENT_DATE()

    UNION ALL

    SELECT
        orders.person_id,
        orders.clinical_effective_date::DATE AS event_date,
        codes.event_type,
        codes.vaccine_group
    FROM {{ ref('stg_olids_medication_order') }} AS orders
    INNER JOIN codes ON orders.mapped_concept_code = codes.code
        AND codes.event_type = 'Administration_drug'
    INNER JOIN population AS p ON orders.person_id = p.person_id
    WHERE orders.clinical_effective_date::DATE BETWEEN DATE_TRUNC('month', p.birth_date_approx) AND CURRENT_DATE()
),

grouped AS (
    SELECT
        p.person_id,
        p.birth_date_approx,
        ev.vaccine_group,
        ev.event_date,
        ev.event_type IN ('Administration', 'Administration_drug') AS is_administered,
        ev.event_type = 'Contraindicated' AS is_contraindicated
    FROM population AS p
    LEFT JOIN events AS ev
        ON p.person_id = ev.person_id
        AND ev.vaccine_group IS NOT NULL
)

SELECT
    person_id,
    birth_date_approx,
    COUNT(DISTINCT CASE WHEN is_administered AND vaccine_group = 'DTAP_PRIMARY'
        AND event_date < DATEADD(month, 8, birth_date_approx) THEN event_date END) AS dtap_doses_by_8_months,
    COUNT(DISTINCT CASE WHEN is_administered AND vaccine_group = 'MMR'
        AND event_date >= DATEADD(month, 12, birth_date_approx)
        AND event_date < DATEADD(month, 18, birth_date_approx)
        THEN event_date END) AS mmr_doses_12_to_18_months,
    COUNT(DISTINCT CASE WHEN is_administered AND vaccine_group = 'MMR'
        AND event_date >= DATEADD(year, 1, birth_date_approx)
        AND event_date < DATEADD(year, 5, birth_date_approx)
        THEN event_date END) AS mmr_doses_1_to_5_years,
    COALESCE(BOOLOR_AGG(is_administered AND vaccine_group = 'DTAP_BOOSTER'
        AND event_date >= DATEADD(year, 1, birth_date_approx)
        AND event_date < DATEADD(year, 5, birth_date_approx)), FALSE) AS has_dtap_booster_1_to_5_years,
    COUNT(DISTINCT CASE WHEN is_administered AND vaccine_group = 'ROTAVIRUS'
        AND event_date < DATEADD(week, 24, birth_date_approx) THEN event_date END) AS rotavirus_doses_by_24_weeks,
    COUNT(DISTINCT CASE WHEN is_administered AND vaccine_group = 'MENB'
        AND event_date < DATEADD(month, 8, birth_date_approx) THEN event_date END) AS menb_doses_by_8_months,
    COUNT(DISTINCT CASE WHEN is_administered AND vaccine_group = 'MENB'
        AND event_date < DATEADD(month, 18, birth_date_approx) THEN event_date END) AS menb_doses_by_18_months,
    COUNT(DISTINCT CASE WHEN is_administered AND vaccine_group = 'MENB'
        AND event_date < DATEADD(month, 12, birth_date_approx) THEN event_date END) AS menb_primary_doses_by_12_months,
    COUNT(DISTINCT CASE WHEN is_administered AND vaccine_group = 'MENB'
        AND event_date >= DATEADD(month, 12, birth_date_approx)
        AND event_date < DATEADD(month, 18, birth_date_approx)
        THEN event_date END) AS menb_booster_doses_12_to_18_months,
    COALESCE(BOOLOR_AGG(is_contraindicated AND vaccine_group IN ('DTAP_PRIMARY', 'DTAP_BOOSTER')), FALSE) AS has_dtap_contraindication,
    COALESCE(BOOLOR_AGG(is_contraindicated AND vaccine_group = 'MMR'), FALSE) AS has_mmr_contraindication,
    COALESCE(BOOLOR_AGG(is_contraindicated AND vaccine_group = 'ROTAVIRUS'), FALSE) AS has_rotavirus_contraindication,
    COALESCE(BOOLOR_AGG(is_contraindicated AND vaccine_group = 'MENB'), FALSE) AS has_menb_contraindication
FROM grouped
GROUP BY person_id, birth_date_approx
