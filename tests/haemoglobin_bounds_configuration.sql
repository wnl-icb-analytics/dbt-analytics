-- Haemoglobin conversion depends on this single maintained parameter row.
SELECT COUNT(*) AS configuration_rows
FROM {{ ref('observation_value_bounds') }}
WHERE definition_name = 'haemoglobin_est'
HAVING COUNT(*) <> 1
    OR COALESCE(COUNT_IF(absolute_lower = 25 AND absolute_upper = 300
        AND canonical_exponent = -1
        AND biological_lower = 25 AND biological_upper = 300), 0) <> 1
