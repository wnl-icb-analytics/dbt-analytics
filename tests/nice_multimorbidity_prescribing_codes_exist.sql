-- Missing Cambridge medication sets would silently remove IND207 categories.
WITH required_conditions AS (
    SELECT column1 AS condition_code FROM VALUES (5065), (5066), (5071)
)
SELECT required.condition_code
FROM required_conditions AS required
LEFT JOIN {{ ref('stg_common_ccmc_dmd') }} AS codes
    ON required.condition_code = codes.conditionid
GROUP BY required.condition_code
HAVING COUNT(codes.productid) = 0

-- depends_on: {{ ref('int_nice_multimorbidity_categories') }}
