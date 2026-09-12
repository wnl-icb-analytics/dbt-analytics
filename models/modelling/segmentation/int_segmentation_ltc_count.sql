{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- LTC count for population segmentation.
-- Grain: one row per person on at least one qualifying LTC register, all ages.
--
-- Feeds the adults with multiple/single LTC segments (6/5) and the children
-- with health needs segment (2), plus the LTC criterion of children with
-- complexity (3). Not used by the complex adults cohort, which keeps its own
-- 16-condition list without NAFLD.
--
-- Condition list: the 16 complex adults conditions plus NAFLD. CYP register
-- variants are canonicalised before counting (CYP_AST -> AST, LD_U14 -> LD)
-- so children on CYP-specific registers count without double counting.
--
-- Learning disability age rule: LD counts at age >= 65 (per the complex
-- adults definition) or under 18 (the adult rule exists because LD alone is
-- not treated as an LTC in working-age adults; for children it indicates
-- health need).

{% set ltc_codes = [
    'AF', 'AST', 'CHD', 'CKD', 'COPD', 'DEM', 'DEP', 'DM',
    'EP', 'HF', 'HTN', 'SMI', 'STIA', 'PD', 'ANX', 'NAFLD'
] %}

WITH canonical AS (
    SELECT
        s.person_id,
        CASE s.condition_code
            WHEN 'CYP_AST' THEN 'AST'
            WHEN 'LD_U14' THEN 'LD'
            ELSE s.condition_code
        END AS condition_code,
        a.age
    FROM {{ ref('fct_person_ltc_summary') }} AS s
    INNER JOIN {{ ref('dim_person_age') }} AS a
        ON s.person_id = a.person_id
)

SELECT
    person_id,
    COUNT(DISTINCT condition_code) AS ltc_count,
    ARRAY_AGG(DISTINCT condition_code) WITHIN GROUP (ORDER BY condition_code)
        AS ltc_list
FROM canonical
WHERE
    condition_code IN ({{ "'" ~ ltc_codes | join("', '") ~ "'" }})
    OR (condition_code = 'LD' AND (age >= 65 OR age < 18))
GROUP BY person_id
