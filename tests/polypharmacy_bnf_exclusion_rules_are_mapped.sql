WITH exclusions AS (
    SELECT
        bnf_chapter_code,
        REPLACE(bnf_chapter_code, '.', '') AS bnf_code_prefix
    FROM {{ ref('stg_reference_bnf_polypharmacy_exclusions') }}
),

unmapped_rules AS (
    SELECT exclusions.bnf_chapter_code
    FROM exclusions
    LEFT JOIN {{ ref('stg_reference_bnf_latest') }} bnf
        ON bnf.bnf_code LIKE exclusions.bnf_code_prefix || '%'
    GROUP BY exclusions.bnf_chapter_code
    HAVING COUNT(bnf.snomed_code) = 0
)

SELECT bnf_chapter_code
FROM unmapped_rules
