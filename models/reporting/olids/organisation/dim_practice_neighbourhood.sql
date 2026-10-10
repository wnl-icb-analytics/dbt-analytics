{{
    config(
        materialized='table',
        tags=['dimension', 'practice', 'geography'],
        cluster_by=['practice_code'])
}}

/*
Practice Neighbourhood Dimension
Provides geographic context for GP practices including local authority and neighbourhood classification.
Note: Working with dummy data so geographic information may be limited/placeholder.
*/

SELECT
    practice_code,
    practice_name,
    registered_borough_name AS local_authority,
    neighbourhood_name AS neighbourhood_registered,
    neighbourhood_code
FROM {{ ref('practice_wnl_active') }}
WHERE sub_icb_code = {{ ncl_sub_icb() }}
