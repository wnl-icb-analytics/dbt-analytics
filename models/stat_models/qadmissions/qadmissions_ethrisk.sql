{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Ethnicity risk group (1..9) per person, for the QAdmissions feature
`ethrisk`. Numbering follows the registered QAdmissions model:

  1 = White / NotRecorded (default)   6 = Black Caribbean
  2 = Indian                          7 = Black African
  3 = Pakistani                       8 = Chinese
  4 = Bangladeshi                     9 = Other Ethnic Group
  5 = Other Asian

Pipeline
  dim_person_ethnicity holds one row per person with their latest
  ethnicity_subcategory (e.g. "White: British", "Asian: Indian", or
  "Not Recorded" for persons with no usable record - the upstream
  COALESCEs missing records). Joining that subcategory to the
  qadmissions_eth2016_to_ethrisk9 seed produces ethrisk for every
  person. The seed includes explicit rows for "Not Recorded" /
  "Not Stated" / "Refused" / "Recorded Not Known" - all mapping to 1
  per the QAdmissions paper convention that NotRecorded shares
  group 1 with White.

  Any subcategory NOT in the seed defaults to 1 (defensive fallback;
  the qadmissions_ethnicity_mapping_complete singular test fails when
  this fires so data drift is surfaced rather than silently absorbed).

Grain: one row per person in dim_person_ethnicity (i.e. one per person
in dim_person).
*/

WITH ethnicity AS (
    SELECT
        person_id,
        ethnicity_subcategory,
        latest_ethnicity_date
    FROM {{ ref('dim_person_ethnicity') }}
)

SELECT
    e.person_id,
    e.ethnicity_subcategory,
    e.latest_ethnicity_date,
    COALESCE(m.ethrisk, 1) AS ethrisk,
    COALESCE(m.qadmissions_label, 'White') AS qadmissions_label
FROM ethnicity e
LEFT JOIN {{ ref('qadmissions_eth2016_to_ethrisk9') }} m
    ON e.ethnicity_subcategory = m.ethnicity_subcategory
