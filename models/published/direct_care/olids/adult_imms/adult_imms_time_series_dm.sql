{{
    config(
        materialized='view',
        tags=['adult_imms']
    )
}}

SELECT *
FROM (
SELECT * 
FROM {{ ref('int_adult_imms_ts_ppv')}}
UNION 
SELECT *
FROM {{ ref('int_adult_imms_ts_rsv')}}
UNION
SELECT *
FROM {{ ref('int_adult_imms_ts_shingles_dose_1')}}
UNION
SELECT *
FROM {{ ref('int_adult_imms_ts_shingles_dose_2')}}
)p