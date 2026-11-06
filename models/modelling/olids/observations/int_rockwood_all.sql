{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All Rockwood Clinical Frailty Scale observations from clinical records.
Includes ALL persons (active, inactive, deceased).
Captures Rockwood scores 1-9 with clinical categorisation.

Rockwood Clinical Frailty Scale:
- 1: Very Fit
- 2: Well
- 3: Managing Well
- 4: Vulnerable
- 5: Mildly Frail
- 6: Moderately Frail
- 7: Severely Frail
- 8: Very Severely Frail
- 9: Terminally Ill

Clinical categorisation:
- 1-3: Fit
- 4: Vulnerable
- 5: Mild Frailty
- 6: Moderate Frailty
- 7-8: Severe Frailty
- 9: Terminally Ill
*/

WITH rockwood_mapping AS (
    SELECT * FROM VALUES
        ('1129331000000101', 1, 'Very Fit', 'Fit'),
        ('1129341000000105', 2, 'Well', 'Fit'),
        ('1129351000000108', 3, 'Managing Well', 'Fit'),
        ('1129361000000106', 4, 'Vulnerable', 'Vulnerable'),
        ('1129371000000104', 5, 'Mildly Frail', 'Mild Frailty'),
        ('1129381000000102', 6, 'Moderately Frail', 'Moderate Frailty'),
        ('1129391000000100', 7, 'Severely Frail', 'Severe Frailty'),
        ('1129401000000102', 8, 'Very Severely Frail', 'Severe Frailty'),
        ('1129411000000100', 9, 'Terminally Ill', 'Terminally Ill')
    AS rockwood_lookup(concept_code, score, description, category)
),

base_observations AS (
    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id
    FROM ({{ get_observations("'ROCKWOOD_SCORES'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
      AND obs.clinical_effective_date <= CURRENT_DATE()
)

SELECT
    bo.person_id,
    bo.id,
    bo.clinical_effective_date,
    bo.concept_code,
    bo.concept_display,
    bo.source_cluster_id,
    
    rm.score AS rockwood_score,
    rm.description AS rockwood_description,
    CONCAT('Level ', rm.score, ': ', rm.description) AS frailty_level,
    rm.category AS frailty_category,
    
    -- Data quality flags
    rm.concept_code IS NOT NULL AS is_valid_rockwood_code,
    rm.score BETWEEN 5 AND 8 AS is_frail,
    rm.score BETWEEN 7 AND 8 AS is_severely_frail

FROM base_observations bo
LEFT JOIN rockwood_mapping rm
    ON bo.concept_code = rm.concept_code

ORDER BY person_id, clinical_effective_date DESC