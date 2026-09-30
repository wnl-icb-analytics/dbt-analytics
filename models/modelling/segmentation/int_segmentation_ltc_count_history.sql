{{
    config(
        materialized='table',
        cluster_by=['end_date', 'person_id'],
        tags=['monthly-full']
    )
}}

-- Monthly LTC count for the wider population segmentation.

WITH qualifying_registers AS (
    SELECT h.*
    FROM {{ ref('int_segmentation_ltc_history') }} AS h
    INNER JOIN {{ ref('int_segmentation_person_month_spine') }} AS pm
        ON h.person_id = pm.person_id
        AND h.end_date = pm.month_end_date
    WHERE
        h.condition_code != 'LD'
        OR pm.age >= 65
        OR pm.age < 18
)

SELECT
    person_id,
    end_date,
    COUNT(DISTINCT condition_code) AS ltc_count,
    ARRAY_AGG(DISTINCT condition_code) WITHIN GROUP (ORDER BY condition_code)
        AS ltc_list
FROM qualifying_registers
GROUP BY person_id, end_date
