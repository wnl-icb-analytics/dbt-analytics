{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Alcohol category (0..5) per person for the QAdmissions feature
`alcohol_cat6`. The QAdmissions model expects the 6-level category from
the ALCOHOL_MAP in the registered Python implementation:

  0 = NonDrinker
  1 = Trivial      (less than 1 unit / day)
  2 = Light        (1-2 units / day)
  3 = Moderate     (3-6 units / day)
  4 = Heavy        (7-9 units / day)
  5 = VeryHeavy    (more than 9 units / day)

Source
  int_alcohol_audit_scores filtered to audit_type = 'Full AUDIT'.
  AUDIT-C scores are deliberately excluded - the AUDIT-C 0..12 bands do
  not align cleanly with the QAdmissions cat6 thresholds, whereas the
  Full AUDIT 0..40 bands map naturally to six categories.

Score-to-category mapping (Full AUDIT 0..40 -> cat6):
  0       -> 0  (NonDrinker)
  1..3    -> 1  (Trivial)
  4..7    -> 2  (Light)
  8..15   -> 3  (Moderate)
  16..19  -> 4  (Heavy)
  20+     -> 5  (VeryHeavy)

Per-person aggregation
  Take the HIGHEST cat6 ever recorded for the person (precautionary -
  prior heavy drinking is treated as informative even if the latest
  AUDIT score is lower). Tie-break by latest clinical_effective_date
  for stability when multiple observations share the same cat6.

Persons with no Full AUDIT record do not appear here; the consuming
int_qadmissions_features model passes NULL through to the registered
QAdmissions model rather than substituting 0 (NonDrinker).

Grain: one row per person who has at least one valid Full AUDIT score.
*/

WITH full_audit AS (
    SELECT
        person_id,
        id AS observation_id,
        clinical_effective_date,
        audit_score,
        CASE
            WHEN audit_score = 0                    THEN 0
            WHEN audit_score BETWEEN  1 AND  3      THEN 1
            WHEN audit_score BETWEEN  4 AND  7      THEN 2
            WHEN audit_score BETWEEN  8 AND 15      THEN 3
            WHEN audit_score BETWEEN 16 AND 19      THEN 4
            WHEN audit_score >= 20                  THEN 5
            ELSE NULL
        END AS alcohol_cat6
    FROM {{ ref('int_alcohol_audit_scores') }}
    WHERE audit_type    = 'Full AUDIT'
      AND is_valid_score = TRUE
)

SELECT
    person_id,
    alcohol_cat6,
    audit_score,
    clinical_effective_date
FROM full_audit
WHERE alcohol_cat6 IS NOT NULL
-- Tertiary keys (audit_score, observation_id) make the row pick
-- deterministic when several Full AUDIT records share the same cat6
-- and date.
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id
    ORDER BY
        alcohol_cat6 DESC,
        clinical_effective_date DESC,
        audit_score DESC,
        observation_id ASC
) = 1
