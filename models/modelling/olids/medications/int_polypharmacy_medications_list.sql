{{
    config(
        materialized='table',
        tags=['polypharmacy']
    )
}}

/*
BNF medications in scope for polypharmacy calculation.
Uses exclusion-based approach with granular subchapter-level filtering.

Exclusions are managed in seed file: bnf_polypharmacy_exclusions.csv
This approach provides:
- Single source of truth for exclusions
- New BNF subchapters are included unless explicitly excluded
- Auditable configuration changes

Key exclusions:
- Chapter 5: Most infections (except HIV 5.3.1 and hepatitis 5.3.3)
- Chapter 9: Vitamins, minerals, nutrition products
- Chapter 6.2: Thyroid drugs
- Chapters 7.1-7.3: Obstetrics, contraceptives
- Chapters 11-15, 18, 20-23: Topicals, devices, dressings
- Various subchapters: cough preparations, stoma care, etc.

Logic:
1. Start with all BNF medications that have valid SNOMED codes
2. Match each BNF code to its most-specific exclusion rule
3. Only include medications not marked as excluded
4. This ensures only drugs with valid BNF mappings in non-excluded chapters are included

Grain: One row per SNOMED code included in polypharmacy scope
*/

WITH bnf_base AS (
    SELECT
        snomed_code,
        bnf_code,
        LEFT(bnf_code, 2) AS bnf_chapter_2digit,
        bnf_name
    FROM {{ ref('stg_reference_bnf_latest') }}
    WHERE snomed_code IS NOT NULL
        AND bnf_code IS NOT NULL
        AND LEN(bnf_code) >= 2  -- Must have at least chapter-level BNF code
),

exclusions AS (
    SELECT
        bnf_chapter_code,
        is_excluded,
        REPLACE(bnf_chapter_code, '.', '') AS bnf_code_prefix
    FROM {{ ref('stg_reference_bnf_polypharmacy_exclusions') }}
),

-- Match dotted seed codes against undotted source BNF codes.
-- The longest matching prefix takes precedence, allowing subchapters to
-- override chapter rules such as the HIV and hepatitis inclusions in chapter 5.
medications_with_exclusion_status AS (
    SELECT
        b.snomed_code,
        b.bnf_code,
        b.bnf_chapter_2digit AS bnf_chapter,
        b.bnf_name,
        COALESCE(e.is_excluded, FALSE) AS is_excluded
    FROM bnf_base b
    LEFT JOIN exclusions e
        ON b.bnf_code LIKE e.bnf_code_prefix || '%'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY b.snomed_code
        ORDER BY LENGTH(e.bnf_code_prefix) DESC NULLS LAST
    ) = 1
)

-- Final list: only medications that are NOT excluded
SELECT
    snomed_code,
    bnf_code,
    bnf_chapter,
    bnf_name
FROM medications_with_exclusion_status
WHERE is_excluded = FALSE
