{{ config(materialized='table', cluster_by=['person_id', 'event_date']) }}

WITH event_codes AS (
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

    SELECT DISTINCT
        code,
        'ROTAVIRUS' AS vaccine_group,
        'Contraindicated' AS event_type
    FROM {{ ref('stg_reference_combined_codesets') }}
    WHERE source = 'PCD'
        AND cluster_id = 'ROTAVACEXC_COD'
        -- The wider cluster also contains PCA reasons, which NICE denominators retain.
        AND code IN (
            '868691000000101', -- Rotavirus vaccination contraindicated
            '885901000000106'  -- History of rotavirus vaccine allergy
        )

    UNION ALL

    -- These QOF drug refsets are not included in the combined PCD code extract.
    SELECT DISTINCT
        referenced_component_id AS code,
        CASE
            WHEN ref_set_id IN ('133231000001105', '2089331000001102') THEN 'MMR'
            ELSE 'DTAP_PRIMARY'
        END AS vaccine_group,
        'Administration_drug' AS event_type
    FROM {{ ref('stg_nhsd_snomed_sct_refset_simple') }}
    WHERE active
        AND ref_set_id IN (
            '72391000001101', -- 4IN1VACDRUG_COD
            '72381000001103', -- 5IN1VACDRUG_COD
            '72371000001100', -- 6IN1VACDRUG_COD
            '133231000001105', -- MMRVACDRUG_COD
            '2089331000001102' -- MMRVVACDRUG_COD
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
        obs.age_at_event,
        codes.event_type,
        codes.vaccine_group
    FROM {{ ref('stg_olids_observation') }} AS obs
    INNER JOIN codes ON obs.mapped_concept_code = codes.code
    WHERE obs.clinical_effective_date::DATE <= CURRENT_DATE()

    UNION ALL

    SELECT
        orders.person_id,
        orders.clinical_effective_date::DATE AS event_date,
        orders.age_at_event,
        codes.event_type,
        codes.vaccine_group
    FROM {{ ref('stg_olids_medication_order') }} AS orders
    INNER JOIN codes ON orders.mapped_concept_code = codes.code
        AND codes.event_type = 'Administration_drug'
    WHERE orders.clinical_effective_date::DATE <= CURRENT_DATE()
)
SELECT person_id, event_date, age_at_event, event_type, vaccine_group
FROM events
WHERE vaccine_group IS NOT NULL
GROUP BY person_id, event_date, age_at_event, event_type, vaccine_group
