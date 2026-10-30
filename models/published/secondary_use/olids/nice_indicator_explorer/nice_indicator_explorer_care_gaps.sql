{{ config(cluster_by=['practice_code']) }}

-- People by number of unmet NICE indicators, for proactive care cohorts.
-- Opted-out people are excluded.

WITH person_gaps AS (
    SELECT
        s.person_id,
        s.reporting_date,
        s.current_practice_code AS practice_code,
        MAX(s.age) AS age,
        COUNT(*) AS indicators_due,
        COUNT_IF(NOT s.is_in_numerator) AS indicators_unmet
    FROM {{ ref('fct_person_nice_indicator_status') }} AS s
    INNER JOIN {{ ref('dim_person_secondary_use_allowed') }} AS a
        ON s.person_id = a.person_id
    WHERE s.is_in_denominator
    GROUP BY s.person_id, s.reporting_date, s.current_practice_code
),

people AS (
    SELECT
        pg.reporting_date,
        pg.practice_code,
        COALESCE(sg.segment_number || ' ' || sg.segment_name, 'Not segmented') AS segment,
        CASE f.category
            WHEN 'ROBUST' THEN 'Fit'
            WHEN 'MILD FRAILTY' THEN 'Mild'
            WHEN 'MODERATE FRAILTY' THEN 'Moderate'
            WHEN 'SEVERE FRAILTY' THEN 'Severe'
            ELSE 'Not scored'
        END AS frailty,
        CASE d.imd_quintile_numeric_25
            WHEN 1 THEN '1 Most deprived'
            WHEN 5 THEN '5 Least deprived'
            ELSE COALESCE(d.imd_quintile_numeric_25::VARCHAR, 'Unknown')
        END AS imd_quintile,
        COALESCE(NULLIF(d.ethnicity_category, ''), 'Unknown') AS ethnicity,
        CASE
            WHEN ch.is_care_home_resident THEN 'Care home'
            WHEN hb.is_housebound THEN 'Housebound'
            ELSE 'Neither recorded'
        END AS residence,
        CASE
            WHEN pg.age IS NULL THEN 'Unknown'
            WHEN pg.age < 18 THEN '0 to 17'
            WHEN pg.age < 40 THEN '18 to 39'
            WHEN pg.age < 65 THEN '40 to 64'
            WHEN pg.age < 75 THEN '65 to 74'
            WHEN pg.age < 85 THEN '75 to 84'
            ELSE '85 and over'
        END AS age_band,
        LEAST(pg.indicators_unmet, 10) AS indicators_unmet,
        pg.indicators_unmet AS unmet_results,
        pg.indicators_due
    FROM person_gaps AS pg
    LEFT JOIN {{ ref('fct_person_segment') }} AS sg
        ON pg.person_id = sg.person_id
    LEFT JOIN {{ ref('fct_person_efi2') }} AS f
        ON pg.person_id = f.person_id
    LEFT JOIN {{ ref('dim_person_demographics') }} AS d
        ON pg.person_id = d.person_id
    LEFT JOIN {{ ref('dim_person_care_home') }} AS ch
        ON pg.person_id = ch.person_id
    LEFT JOIN {{ ref('dim_person_housebound_status') }} AS hb
        ON pg.person_id = hb.person_id
),

grouped AS (
    SELECT
        reporting_date,
        practice_code,
        segment,
        frailty,
        imd_quintile,
        ethnicity,
        age_band,
        residence,
        indicators_unmet,
        COUNT(*) AS people,
        SUM(unmet_results) AS unmet_results,
        SUM(indicators_due) AS indicators_due
    FROM people
    GROUP BY ALL
)

SELECT
    g.reporting_date,
    g.practice_code,
    p.practice_name,
    p.pcn_code,
    p.pcn_name,
    pn.neighbourhood_code,
    pn.neighbourhood_registered AS neighbourhood_name,
    p.borough_registered AS borough,
    g.segment,
    g.frailty,
    g.imd_quintile,
    g.ethnicity,
    g.age_band,
    g.residence,
    g.indicators_unmet,
    g.people,
    g.unmet_results,
    g.indicators_due
FROM grouped AS g
LEFT JOIN {{ ref('dim_practice') }} AS p
    ON g.practice_code = p.practice_code
LEFT JOIN {{ ref('dim_practice_neighbourhood') }} AS pn
    ON g.practice_code = pn.practice_code
