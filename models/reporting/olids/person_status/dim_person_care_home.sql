{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'care_home', 'residence'],
        cluster_by=['person_id'])
}}

-- Person Care Home Dimension Table
-- Holds the latest care home/nursing home status for persons
-- Only includes persons who have a recorded care home or temporary care home status
-- Uses CAREHOME_COD and TEMPCARHOME_COD clusters.
-- NURSEHOME_COD is excluded: its only code is "Employed by nursing home" (a staff
-- code, not a residency code). A handful of staff codes mixed into CAREHOME_COD
-- (employed by/matron/residential child or youth care worker) are excluded below
-- for the same reason - see issue #840.

WITH care_home_observations AS (
    SELECT *
    FROM (
        {{ get_observations("'CAREHOME_COD', 'TEMPCARHOME_COD'") }}
    )
    WHERE mapped_concept_code NOT IN (
        '1092561000000107', -- Employed by care home
        '158944006', -- Matron (old people's home)
        '158942005', -- Residential child care worker
        '158943000' -- Residential youth care worker
    )
),

observation_clusters AS (
    -- First, collect all cluster IDs and determine residence status for each observation
    SELECT
        o.ID,
        ARRAY_AGG(DISTINCT o.cluster_id) WITHIN GROUP (ORDER BY o.cluster_id) AS cluster_ids,
        -- Pre-calculate residence status based on clusters
        -- Nursing home is a sub-type of CAREHOME_COD codes (no separate residency
        -- cluster exists for it), identified by code description
        CASE
            WHEN ARRAY_CONTAINS('CAREHOME_COD'::VARIANT, ARRAY_AGG(DISTINCT o.cluster_id))
                AND BOOLOR_AGG(o.code_description ILIKE '%nursing home%') THEN 'Nursing Home'
            WHEN ARRAY_CONTAINS('CAREHOME_COD'::VARIANT, ARRAY_AGG(DISTINCT o.cluster_id)) THEN 'Care Home'
            WHEN ARRAY_CONTAINS('TEMPCARHOME_COD'::VARIANT, ARRAY_AGG(DISTINCT o.cluster_id)) THEN 'Temporary Care Home'
            ELSE NULL
        END AS residence_type,
        -- Determine if temporary
        ARRAY_CONTAINS('TEMPCARHOME_COD'::VARIANT, ARRAY_AGG(DISTINCT o.cluster_id)) AS is_temporary
    FROM care_home_observations o
    GROUP BY o.ID
),

latest_residence_status_per_person AS (
    -- Then get the latest observation per person with all its details
    SELECT
        o.person_id,
        pp.sk_patient_id,
        o.clinical_effective_date,
        o.mapped_concept_id AS concept_id,
        o.mapped_concept_code AS concept_code,
        o.mapped_concept_display AS term,
        -- Determine residence status
        oc.residence_type IS NOT NULL AS is_care_home_resident,
        oc.residence_type = 'Nursing Home' AS is_nursing_home_resident,
        oc.is_temporary AS is_temporary_resident,
        oc.residence_type AS residence_type,
        CASE
            WHEN oc.is_temporary THEN 'Temporary'
            ELSE 'Permanent'
        END AS residence_status,
        oc.cluster_ids AS source_cluster_ids,
        o.ID AS observation_lds_id -- Include for potential tie-breaking
    FROM care_home_observations o
    JOIN {{ ref('int_patient_person_unique') }} pp
        ON o.patient_id = pp.patient_id
    JOIN observation_clusters oc
        ON o.ID = oc.ID
    WHERE oc.residence_type IS NOT NULL -- Only include records with a valid residence type
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY o.person_id
            -- Order by date first, then by observation ID as a tie-breaker
            ORDER BY o.clinical_effective_date DESC, o.ID DESC
        ) = 1 -- Get only the latest record per person
)

-- Select only persons with a care home record
SELECT
    lrsp.person_id,
    lrsp.sk_patient_id,
    lrsp.clinical_effective_date AS latest_residence_date,
    lrsp.concept_id,
    lrsp.concept_code,
    lrsp.term,
    lrsp.is_care_home_resident,
    lrsp.is_nursing_home_resident,
    lrsp.is_temporary_resident,
    lrsp.residence_type,
    lrsp.residence_status,
    lrsp.source_cluster_ids
FROM latest_residence_status_per_person lrsp
