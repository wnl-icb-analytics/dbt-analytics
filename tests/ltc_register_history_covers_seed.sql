-- Every condition in ltc_register_denominator_rules has a monthly register model in
-- fct_person_ltc_summary_by_month, and every model's code is in the seed.

WITH history_codes AS (
    {% for condition_code, register_model in ltc_register_history_models() %}
    SELECT '{{ condition_code }}' AS condition_code
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),

seed_codes AS (
    SELECT UPPER(condition_code) AS condition_code
    FROM {{ ref('ltc_register_denominator_rules') }}
)

SELECT 'missing_from_history' AS issue, seed_codes.condition_code
FROM seed_codes
LEFT JOIN history_codes ON seed_codes.condition_code = history_codes.condition_code
WHERE history_codes.condition_code IS NULL

UNION ALL

SELECT 'missing_from_seed' AS issue, history_codes.condition_code
FROM history_codes
LEFT JOIN seed_codes ON history_codes.condition_code = seed_codes.condition_code
WHERE seed_codes.condition_code IS NULL
