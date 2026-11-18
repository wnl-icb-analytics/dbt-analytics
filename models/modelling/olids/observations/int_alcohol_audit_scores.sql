{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All AUDIT and AUDIT-C alcohol screening scores from observations.
Includes ALL persons (active, inactive, deceased).
Captures both AUDIT-C (brief 3-question screen) and full AUDIT (10-question assessment).

UK AUDIT-C Scoring (0-12):
- 0: Non-drinker
- 1-2: Minimal/occasional drinking  
- 3-4: Lower risk drinking
- 5-7: Increasing risk
- 8-10: Higher risk
- 11-12: Possible dependence
- Score ≥5 is positive screen indicating potential harm

UK Full AUDIT Scoring (0-40):
- 0: Non-drinker
- 1-3: Minimal/occasional drinking
- 4-7: Lower risk drinking
- 8-15: Increasing risk
- 16-19: Higher risk
- 20+: Possible dependence
*/

WITH base_observations AS (
    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.date_recorded,
        CAST(obs.result_value AS NUMBER(10,2)) AS audit_score,
        obs.result_unit_display,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        obs.result_value AS original_result_value

    FROM ({{ get_observations("'AUDITC_COD', 'AUDIT_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
      AND obs.result_value IS NOT NULL
      AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
      AND obs.age_at_event >= 16 -- Population health threshold: exclude alcohol observations recorded before age 16
),

observations_with_demographics AS (
    SELECT 
        bo.*,
        pd.gender
    FROM base_observations bo
    LEFT JOIN {{ ref('dim_person_demographics') }} pd
        ON bo.person_id = pd.person_id
),
scored AS (
    SELECT *,
        -- AUDIT totals use whole points: AUDIT-C 0-12; full AUDIT 0-40.
        COALESCE(
            original_result_value = FLOOR(original_result_value)
            AND ((source_cluster_id = 'AUDITC_COD' AND original_result_value BETWEEN 0 AND 12)
              OR (source_cluster_id = 'AUDIT_COD' AND original_result_value BETWEEN 0 AND 40)),
            FALSE) AS is_valid_score
    FROM observations_with_demographics
)

SELECT
    person_id,
    ID,
    clinical_effective_date,
    date_recorded,
    audit_score,
    result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    original_result_value,
    gender,

    -- Determine AUDIT type
    CASE 
        WHEN source_cluster_id = 'AUDITC_COD' THEN 'AUDIT-C'
        WHEN source_cluster_id = 'AUDIT_COD' THEN 'Full AUDIT'
        ELSE 'Unknown'
    END AS audit_type,

    is_valid_score,

    -- Risk categorisation using UK AUDIT thresholds with detailed lower end
    CASE
        WHEN source_cluster_id = 'AUDITC_COD' THEN
            CASE
                WHEN NOT is_valid_score THEN 'Invalid'
                WHEN audit_score = 0 THEN 'Non-Drinker'
                WHEN audit_score <= 2 THEN 'Occasional Drinker'
                WHEN audit_score <= 4 THEN 'Lower Risk'
                WHEN audit_score <= 7 THEN 'Increasing Risk'
                WHEN audit_score <= 10 THEN 'Higher Risk'
                ELSE 'Possible Dependence'
            END
        WHEN source_cluster_id = 'AUDIT_COD' THEN
            CASE
                WHEN NOT is_valid_score THEN 'Invalid'
                WHEN audit_score = 0 THEN 'Non-Drinker'
                WHEN audit_score <= 3 THEN 'Occasional Drinker'
                WHEN audit_score <= 7 THEN 'Lower Risk'
                WHEN audit_score <= 15 THEN 'Increasing Risk'
                WHEN audit_score <= 19 THEN 'Higher Risk'
                ELSE 'Possible Dependence'
            END
        ELSE 'Unknown'
    END AS risk_category,

    -- Simplified risk level (for easier aggregation)
    CASE
        WHEN NOT is_valid_score THEN NULL
        WHEN source_cluster_id IN ('AUDITC_COD', 'AUDIT_COD') THEN
            CASE
                WHEN audit_score = 0 THEN 'Non-Drinker'
                WHEN source_cluster_id = 'AUDITC_COD' THEN
                    CASE
                        WHEN audit_score <= 4 THEN 'Lower Risk'
                        WHEN audit_score <= 7 THEN 'Increasing Risk'
                        ELSE 'Higher Risk'
                    END
                WHEN source_cluster_id = 'AUDIT_COD' THEN
                    CASE
                        WHEN audit_score <= 7 THEN 'Lower Risk'
                        WHEN audit_score <= 15 THEN 'Increasing Risk'
                        ELSE 'Higher Risk'
                    END
            END
    END AS simplified_risk_level,

    -- Existing thresholds apply only to valid scores.
    is_valid_score AND source_cluster_id = 'AUDITC_COD' AND audit_score >= 5 AS auditc_positive_screen,
    is_valid_score AND ((source_cluster_id = 'AUDITC_COD' AND audit_score >= 8)
        OR (source_cluster_id = 'AUDIT_COD' AND audit_score >= 16)) AS requires_specialist_intervention,
    is_valid_score AND ((source_cluster_id = 'AUDITC_COD' AND audit_score >= 5)
        OR (source_cluster_id = 'AUDIT_COD' AND audit_score >= 8)) AS brief_intervention_indicated

FROM scored

-- Sort for consistent output
ORDER BY person_id, clinical_effective_date DESC
