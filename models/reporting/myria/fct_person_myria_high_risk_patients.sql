{{ 
    config(
        materialized='table',
        tags='daily')
    }}

select 
    *
from 
    {{ ref("fct_person_ncl_high_risk_patients") }} 
where
    barnet_hospital_count >= 1
    AND local_authority IN ('Barnet','Enfield')
    AND date_of_death IS NULL