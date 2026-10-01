-- A current non-smoker with unknown history cannot support a never-smoker exemption.
SELECT id
FROM {{ ref('int_smoking_status_all') }}
WHERE concept_code = '405746006'
    AND (
        smoking_status <> 'Non-Smoker (History Unknown)'
        OR is_smoker_code
        OR is_ex_smoker_code
        OR is_never_smoked_code
        OR is_current_smoker
        OR is_ex_smoker
        OR is_never_smoker
        OR has_smoking_history
    )
