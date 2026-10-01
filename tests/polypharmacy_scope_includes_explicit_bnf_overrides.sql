WITH inclusion_overrides AS (
    SELECT
        bnf_chapter_code,
        REPLACE(bnf_chapter_code, '.', '') AS bnf_code_prefix
    FROM {{ ref('stg_reference_bnf_polypharmacy_exclusions') }}
    WHERE NOT is_excluded
),

missing_overrides AS (
    SELECT
        overrides.bnf_chapter_code
    FROM inclusion_overrides overrides
    LEFT JOIN {{ ref('int_polypharmacy_medications_list') }} scope
        ON scope.bnf_code LIKE overrides.bnf_code_prefix || '%'
    GROUP BY overrides.bnf_chapter_code
    HAVING COUNT(scope.snomed_code) = 0
)

SELECT bnf_chapter_code
FROM missing_overrides
