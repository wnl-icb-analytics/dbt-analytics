{{
    config(
        materialized='view',
        tags=['adult_imms']
    )
}}
--Current practice list for all projects
SELECT DISTINCT BOROUGH_REGISTERED, NEIGHBOURHOOD_REGISTERED, PCN_NAME, PRACTICE_CODE, PRACTICE_NAME 
FROM {{ ref('dim_person_demographics') }} 
WHERE IS_ACTIVE=TRUE ORDER BY 1