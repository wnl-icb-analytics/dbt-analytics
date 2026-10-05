{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All chronic liver disease and chronic pancreatitis diagnosis observations
from clinical records.

Uses the QAdmissions 2023 cluster:
- LIVER_DISEASE_OR_CHRONIC_PANCREATITIS_COD_Q: Combined codelist covering
  chronic liver disease (including cirrhosis) and chronic pancreatitis,
  sourced from QResearch as a single set per the QAdmissions paper.

This intermediate is the sole source for the QAdmissions feature
`b_liverpancreas`. It does NOT include pancreatic neoplasms - cancer is
covered by `b_anycancer` via dim_person_conditions.has_cancer.

No resolution codes - diagnosis-only register. No age restrictions applied.

Includes ALL persons (active, inactive, deceased) following intermediate
layer principles. This is OBSERVATION-LEVEL data - one row per coded
diagnosis. Use this model as input for int_qadmissions_features.sql,
where the wide feature row aggregates to the person grain.
*/

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code    AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id             AS source_cluster_id,
    TRUE                       AS is_diagnosis_code,
    'Liver Disease or Chronic Pancreatitis Diagnosis'
                               AS liver_pancreatitis_observation_type

FROM ({{ get_observations("'LIVER_DISEASE_OR_CHRONIC_PANCREATITIS_COD_Q'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, id
