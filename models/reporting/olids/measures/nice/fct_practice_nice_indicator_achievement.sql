{{ config(materialized='table') }}

WITH counts AS (
    SELECT
        reporting_date,
        indicator_id,
        MAX(indicator_name) AS indicator_name,
        MAX(indicator_description) AS indicator_description,
        MAX(denominator_description) AS denominator_description,
        current_practice_code AS practice_code,
        COUNT(*) AS denominator,
        SUM(CASE WHEN is_in_numerator THEN 1 ELSE 0 END) AS numerator
    FROM {{ ref('fct_person_nice_indicator_status') }}
    WHERE is_in_denominator
    GROUP BY reporting_date, indicator_id, current_practice_code
)

SELECT
    c.reporting_date,
    c.indicator_id,
    c.indicator_name,
    i.programme,
    i.clinical_domain,
    i.clinical_subdomain,
    i.name_short,
    c.indicator_description,
    c.denominator_description,
    c.practice_code,
    p.practice_name,
    p.pcn_code,
    p.pcn_name,
    p.borough_registered AS borough,
    c.denominator,
    c.numerator,
    c.denominator - c.numerator AS not_achieved,
    ROUND(100.0 * c.numerator / c.denominator, 1) AS achievement_pct
FROM counts c
LEFT JOIN {{ ref('def_indicator') }} i ON c.indicator_id = i.indicator_id
LEFT JOIN {{ ref('dim_practice') }} p ON c.practice_code = p.practice_code
