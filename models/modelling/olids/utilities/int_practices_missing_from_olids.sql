{{
    config(
        materialized='table',
        tags=['data_quality', 'utilities', 'olids']
    )
}}

/*
Practices Missing from OLIDS

Identifies practices present in the reference practice lookup but with no patients
registered in OLIDS demographics.

This may indicate:
- New practices not yet in OLIDS
- Practices with data feed issues
- Closed practices still in reference data
- Configuration or mapping problems

Uses the practice neighbourhood lookup as the master list of expected practices.
*/

SELECT
    l.practice_code,
    l.practice_name,
    l.registered_borough_name as local_authority,
    l.neighbourhood_name as practice_neighbourhood
FROM {{ ref('practice_wnl_active') }} l
LEFT JOIN {{ ref('dim_person_demographics') }} d
    ON d.practice_code = l.practice_code
WHERE d.practice_code IS NULL
    AND l.sub_icb_code = {{ ncl_sub_icb() }}
