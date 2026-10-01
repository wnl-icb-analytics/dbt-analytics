WITH exclusions AS (
    SELECT
        bnf_chapter_code,
        is_excluded,
        REPLACE(bnf_chapter_code, '.', '') AS bnf_code_prefix
    FROM {{ ref('stg_reference_bnf_polypharmacy_exclusions') }}
),

matching_rules AS (
    SELECT
        scope.snomed_code,
        scope.bnf_code,
        exclusions.bnf_chapter_code,
        exclusions.is_excluded
    FROM {{ ref('int_polypharmacy_medications_list') }} scope
    INNER JOIN exclusions
        ON scope.bnf_code LIKE exclusions.bnf_code_prefix || '%'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY scope.snomed_code
        ORDER BY LENGTH(exclusions.bnf_code_prefix) DESC
    ) = 1
)

SELECT
    snomed_code,
    bnf_code,
    bnf_chapter_code
FROM matching_rules
WHERE is_excluded
