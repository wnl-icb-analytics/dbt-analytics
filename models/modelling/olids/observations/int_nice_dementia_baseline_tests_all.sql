{{ config(materialized='table', cluster_by=['person_id', 'event_date']) }}
WITH observations AS (
    SELECT id, person_id, clinical_effective_date, cluster_id
    FROM ({{ get_observations("'FBC_COD', 'HB_COD', 'CALC_COD', 'GLUC_COD', 'IFCCHBAM_COD', 'DCCTHBA1C_COD', 'UE_COD', 'CRE_COD', 'EGFR_COD', 'LFT_COD', 'TFT_COD'", source='PCD') }}) AS pcd
    UNION ALL
    SELECT id, person_id, clinical_effective_date, cluster_id
    FROM ({{ get_observations("'ARDENS/B12_LEVEL', 'ARDENS/FOLATE_LEVEL'", source='OPENCODELISTS') }}) AS ardens
), classified AS (
    SELECT id, person_id, clinical_effective_date::DATE AS event_date,
        CASE cluster_id
            WHEN 'FBC_COD' THEN 'fbc'
            WHEN 'HB_COD' THEN 'fbc'
            WHEN 'CALC_COD' THEN 'calcium'
            WHEN 'GLUC_COD' THEN 'glucose'
            WHEN 'IFCCHBAM_COD' THEN 'glucose'
            WHEN 'DCCTHBA1C_COD' THEN 'glucose'
            WHEN 'UE_COD' THEN 'renal'
            WHEN 'CRE_COD' THEN 'renal'
            WHEN 'EGFR_COD' THEN 'renal'
            WHEN 'LFT_COD' THEN 'liver'
            WHEN 'TFT_COD' THEN 'thyroid'
            WHEN 'ARDENS/B12_LEVEL' THEN 'b12'
            WHEN 'ARDENS/FOLATE_LEVEL' THEN 'folate'
        END AS test_type
    FROM observations
    WHERE clinical_effective_date::DATE <= CURRENT_DATE()
)
SELECT id, person_id, event_date, test_type
FROM classified
QUALIFY ROW_NUMBER() OVER (PARTITION BY id, test_type ORDER BY event_date DESC) = 1
