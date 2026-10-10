-- A coded HbA1c test remains evidence when its result is missing.
WITH value_free_tests AS (
    SELECT obs.id, obs.cluster_id
    FROM ({{ get_observations("'IFCCHBAM_COD', 'DCCTHBA1C_COD'") }}) AS obs
    WHERE obs.result_value IS NULL
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= CURRENT_DATE()
)

SELECT tests.id, tests.cluster_id
FROM value_free_tests AS tests
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ ref('int_hba1c_all') }} AS hba
    WHERE hba.id = tests.id
        AND hba.source_cluster_id = tests.cluster_id
)
