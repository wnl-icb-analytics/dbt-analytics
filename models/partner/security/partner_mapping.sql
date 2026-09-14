{{
    config(
        materialized='table',
        cluster_by=['sk_patient_id']
        )
}}
/* Partner Mapping Table. Used as the basis for row level access policies for partner users.

Patients are mapped to a local authority partner if they are either registered with a GP practice in that borough, OR if they are resident in that borough.
This is because local authorities provide services to all their residents, and suppoort practices in their boroughs
so both groups of patients are relevant to the local authority.

*/

WITH REGISTERED_OR_RESIDENT AS (
SELECT SK_PATIENT_ID
,REGISTERED_BOROUGH AS BOROUGH
FROM {{ref('dim_person_demographics_basic')}}

UNION

SELECT SK_PATIENT_ID
,RESIDENCE_BOROUGH AS BOROUGH
FROM {{ref('dim_person_demographics_basic')}}
)

SELECT SK_PATIENT_ID
,'PARTNER__' || UPPER(REPLACE(BOROUGH, ' ', '_')) AS PARTNER_ROLE
FROM REGISTERED_OR_RESIDENT
WHERE BOROUGH NOT IN ('Out of area', 'Unknown')
