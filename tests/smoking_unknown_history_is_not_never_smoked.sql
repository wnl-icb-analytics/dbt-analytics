-- Status-free habit codes are excluded; unknown past history is not never-smoking.
SELECT id
FROM {{ ref('int_smoking_status_all') }}
WHERE (source_cluster_id = 'SMOK_COD' AND concept_code <> '405746006')
    OR (concept_code = '405746006' AND (
        smoking_status <> 'Non-Smoker (History Unknown)'
        OR is_smoker_code
        OR is_ex_smoker_code
        OR is_never_smoked_code
        OR is_current_smoker
        OR is_ex_smoker
        OR is_never_smoker
        OR has_smoking_history
    ))
