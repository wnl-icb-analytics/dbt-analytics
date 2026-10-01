SELECT bnf_chapter_code
FROM {{ ref('stg_reference_bnf_polypharmacy_exclusions') }}
WHERE NOT REGEXP_LIKE(bnf_chapter_code, '^[0-9]{2}(\\.[0-9]{2}){0,2}$')
