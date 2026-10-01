{{
    config(
        materialized='table',
        tags=['intermediate', 'organisation', 'borough', 'mapping'],
        cluster_by=['practice_code'])
}}

/*
Organisation Borough Mapping
Maps practices and PCNs to London boroughs and legacy sub-ICBs using ODS.

Borough: derived from historic CCG relationships in OrganisationDescendent. Contractual
view (how practices were commissioned through NHSE), not strict geography — boundary
practices stay with the CCG that contracted them. The CCG -> borough lookup is derived
from Dictionary.Organisation (RO98 records with London-ICB descendent paths); the CCG
name is parsed to borough with three hand-mapped exceptions (City and Hackney,
West London, Central London (Westminster)).

Sub-ICB: the RO98/RO319 sub-ICB location the practice is commissioned through —
the commissioner code itself (93C = North Central London, W2U3Z = North West
London). These are the live codes national datasets (SLAM/SUS/EPD) carry; 93C
and W2U3Z survived the 2026-04 ICB merger and were re-parented to Z9B2Z. Names
are curated for the WNL pair (the post-merger ODS names are unreadable, e.g.
"NHS WEST AND NORTH LONDON ICB - 93C").

Legacy ICB: the pre-2026-04-01 ICB identity (QMJ = NCL, QRV = NWL) from the
sub_icb_to_legacy_icb seed. ODS carried these RO261 edges after the merger
but has since purged them from OrganisationDescendent (the sub-ICB locations
now have no ancestor rows at all), so the dissolved hierarchy is pinned as
static reference data. For non-merged ICBs the legacy ICB equals the ICB
itself.

Special handling:
- Medicus Select Care (Y03103) manually assigned to Enfield borough.
- One row per practice for borough_registered (most recent CCG relationship wins).
*/

WITH borough_ccgs AS (
    -- London CCG -> borough lookup derived from Dictionary.Organisation.
    -- Selects RO98 records whose descendent paths pass through a London ICB and
    -- parses the borough from the CCG name. Three CCGs have non-standard names
    -- that don''t reduce to a single borough string; they are hand-mapped.
    SELECT DISTINCT
        org.organisation_code AS ccg_code,
        CASE org.organisation_code
            WHEN '07T' THEN 'Hackney'                 -- NHS City and Hackney CCG
            WHEN '08Y' THEN 'Kensington and Chelsea'  -- NHS West London CCG (K&C + Queens Park/Paddington)
            WHEN '09A' THEN 'Westminster'             -- NHS Central London (Westminster) CCG
            ELSE TRIM(REPLACE(REPLACE(org.organisation_name, 'NHS ', ''), ' CCG', ''))
        END AS borough
    FROM {{ ref('stg_dictionary_dbo_organisation') }} org
    WHERE org.organisation_primary_role = 'RO98'
      AND org.organisation_name LIKE 'NHS % CCG'
      AND EXISTS (
          SELECT 1 FROM {{ ref('stg_dictionary_dbo_organisationdescendent') }} od
          WHERE od.organisation_code_child = org.organisation_code
            AND (od.path LIKE '%[QMJ]%' OR od.path LIKE '%[QRV]%'
              OR od.path LIKE '%[QMF]%' OR od.path LIKE '%[QKK]%'
              OR od.path LIKE '%[QWE]%' OR od.path LIKE '%[Z9B2Z]%')
      )
),

-- Practice to PCN + commissioner from current MatrixView
practice_pcn AS (
    SELECT DISTINCT
        practice_code,
        network_code,
        network_name,
        commissioner_code,
        commissioner_name,
        -- live sub-ICB location: the commissioner itself, except 3 stale
        -- MatrixView rows that still carry the dissolved Brent CCG (07P) as
        -- commissioner - remapped to its successor sub-ICB location W2U3Z
        CASE WHEN commissioner_code = '07P' THEN 'W2U3Z'
             ELSE commissioner_code END AS sub_icb_location_code
    FROM {{ ref('stg_dictionary_dbo_organisationmatrixpracticeview') }}
    WHERE practice_code IS NOT NULL
        AND network_code IS NOT NULL
        AND stp_code IN ('Z9B2Z', 'QMJ', 'QMF', 'QRV', 'QWE', 'QKK')  -- All London ICBs (Z9B2Z = merged WNL from Apr 2026; QMJ/QRV retained as legacy)
),

-- Commissioner (RO98) -> pre-merger ICB from static seed. Previously derived
-- from OrganisationDescendent RO261 parent edges; ODS has purged those edges
-- so the mapping is pinned as reference data. A new commissioner code missing
-- from the seed surfaces as a null legacy_icb_code via the not_null test.
commissioner_to_legacy_icb AS (
    SELECT
        sub_icb_code AS commissioner_code,
        legacy_icb_code,
        legacy_icb_name
    FROM {{ ref('sub_icb_to_legacy_icb') }}
),

-- Sub-ICB display names: curated for the WNL pair (post-merger ODS names are
-- unreadable); the dictionary commissioner name passes through for the rest.
sub_icb_display AS (
    SELECT sub_icb_location_code,
        CASE sub_icb_location_code
            WHEN '93C'   THEN 'NHS North Central London'
            WHEN 'W2U3Z' THEN 'NHS North West London'
            ELSE MIN(commissioner_name)
        END AS sub_icb_name
    FROM practice_pcn
    GROUP BY sub_icb_location_code
),

-- Historic CCG paths from OrganisationDescendent (for borough)
borough_registered_mapping AS (
    SELECT DISTINCT
        od.organisation_code_child AS practice_code,
        bc.ccg_code AS historic_ccg,
        bc.borough,
        od.relationship_start_date,
        od.relationship_end_date,
        od.path,
        ROW_NUMBER() OVER (
            PARTITION BY od.organisation_code_child, bc.borough
            ORDER BY od.relationship_start_date DESC NULLS LAST
        ) AS rn
    FROM {{ ref('stg_dictionary_dbo_organisationdescendent') }} od
    INNER JOIN borough_ccgs bc
        ON od.path LIKE '%[' || bc.ccg_code || ']%'
    WHERE od.organisation_primary_role_child = 'RO177' -- GP Practice
),

-- One borough per practice: active relationships first, then most recent end date,
-- then most recent start date. Eliminates fanout where a practice historically
-- sat under multiple CCGs in different boroughs.
borough_registered_final AS (
    SELECT
        practice_code,
        CASE
            WHEN practice_code = 'Y03103' THEN 'Enfield'  -- Medicus Select Care override
            ELSE borough
        END AS borough,
        historic_ccg
    FROM (
        SELECT
            practice_code,
            borough,
            historic_ccg,
            relationship_end_date,
            relationship_start_date,
            ROW_NUMBER() OVER (
                PARTITION BY practice_code
                ORDER BY
                    CASE WHEN relationship_end_date IS NULL THEN 0 ELSE 1 END,
                    relationship_end_date DESC NULLS LAST,
                    relationship_start_date DESC NULLS LAST
            ) AS practice_rn
        FROM borough_registered_mapping
        WHERE rn = 1
            AND (practice_code <> 'Y03103' OR borough = 'Enfield')
    ) ranked
    WHERE practice_rn = 1
),

-- PCN borough: majority borough across members (pcn-grain dedup)
pcn_borough_mapping AS (
    SELECT
        pp.network_code,
        pbf.borough,
        COUNT(DISTINCT pbf.practice_code) AS borough_practice_count,
        ROW_NUMBER() OVER (
            PARTITION BY pp.network_code
            ORDER BY COUNT(DISTINCT pbf.practice_code) DESC
        ) AS borough_rank
    FROM practice_pcn pp
    INNER JOIN borough_registered_final pbf
        ON pp.practice_code = pbf.practice_code
    WHERE pp.network_code IS NOT NULL
    GROUP BY pp.network_code, pbf.borough
),

pcn_borough_final AS (
    SELECT network_code, borough, borough_practice_count
    FROM pcn_borough_mapping
    WHERE borough_rank = 1
),

-- One commissioner per PCN (verified: every PCN has a single commissioner in MatrixView)
pcn_commissioner AS (
    SELECT DISTINCT network_code, commissioner_code, sub_icb_location_code
    FROM practice_pcn
    WHERE network_code IS NOT NULL
)

-- Driven by current London practices from MatrixView. Historic borough is decorative.
SELECT
    pp.practice_code,
    pbf.borough AS borough_registered,
    pbf.historic_ccg AS practice_historic_ccg,
    pp.commissioner_code,
    -- live RO98/RO319 sub-ICB location: the commissioner itself (93C / W2U3Z)
    pp.sub_icb_location_code AS sub_icb_code,
    sd.sub_icb_name,
    cs.legacy_icb_code,
    cs.legacy_icb_name,

    pp.network_code,
    pcnbf.borough AS pcn_borough,
    pcnbf.borough_practice_count AS pcn_borough_practice_count,
    pcm.sub_icb_location_code AS pcn_sub_icb_code,
    psd.sub_icb_name AS pcn_sub_icb_name,
    pcs.legacy_icb_code AS pcn_legacy_icb_code,
    pcs.legacy_icb_name AS pcn_legacy_icb_name

FROM practice_pcn pp
LEFT JOIN borough_registered_final pbf
    ON pp.practice_code = pbf.practice_code
-- legacy lineage keyed on the remapped sub-ICB location, so stale CCG
-- commissioners (07P) inherit their successor's RO261 parent rather than
-- depending on their own edges
LEFT JOIN commissioner_to_legacy_icb cs
    ON pp.sub_icb_location_code = cs.commissioner_code
LEFT JOIN sub_icb_display sd
    ON pp.sub_icb_location_code = sd.sub_icb_location_code
LEFT JOIN pcn_borough_final pcnbf
    ON pp.network_code = pcnbf.network_code
LEFT JOIN pcn_commissioner pcm
    ON pp.network_code = pcm.network_code
LEFT JOIN commissioner_to_legacy_icb pcs
    ON pcm.sub_icb_location_code = pcs.commissioner_code
LEFT JOIN sub_icb_display psd
    ON pcm.sub_icb_location_code = psd.sub_icb_location_code
