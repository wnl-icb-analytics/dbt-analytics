{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Townsend Deprivation Score per person, for the QAdmissions feature `town`.

Each person's residential LSOA is captured on dim_person_demographics as
the 2021 boundary code (lsoa_code_21). The Townsend reference seed is
keyed on 2011 boundaries, so we bridge via stg_reference_lsoa2011_lsoa2021
and then look up the score in qadmissions_townsend_lsoa_2011.

Merged-LSOA handling
  In the ~3% of cases where one 2021 LSOA was formed by merging multiple
  2011 LSOAs, the join produces several rows per person. We take the
  AVG(townsend_score), which approximates the area-weighted score (the
  bridge does not carry area weights, so AVG with equal weight is the
  closest deterministic estimate).

Missing / unmatched persons
  Persons with no recorded LSOA, or whose LSOA does not match the bridge
  (e.g. Scottish, Welsh, or NI-prefixed codes outside the
  England-and-Wales lookup), receive NULL townsend_score. The downstream
  feature model passes the NULL through to the registered QAdmissions
  model rather than falling back to a national-mean default.

Grain: one row per person_id in dim_person_demographics.
*/

WITH persons AS (
    SELECT
        person_id,
        lsoa_code_21
    FROM {{ ref('dim_person_demographics') }}
),

bridged AS (
    SELECT
        p.person_id,
        b.lsoa11_cd
    FROM persons p
    LEFT JOIN {{ ref('stg_reference_lsoa2011_lsoa2021') }} b
        ON p.lsoa_code_21 = b.lsoa21_cd
)

SELECT
    bridged.person_id,
    AVG(t.townsend_score) AS townsend_score
FROM bridged
LEFT JOIN {{ ref('qadmissions_townsend_lsoa_2011') }} t
    ON bridged.lsoa11_cd = t.lsoa_2011_code
GROUP BY bridged.person_id
