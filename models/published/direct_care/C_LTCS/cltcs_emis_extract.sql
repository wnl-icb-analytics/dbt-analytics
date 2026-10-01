{{
    config(
        materialized='table',
        tags=['published', 'direct_care', 'c_ltcs', 'emis_extract']
    )
}}

with 
emis as (
    select * from {{ ref('stg_cltcs_emis') }}
),

registered_practice as (
    select
        cast(sk_patient_id as varchar) as sk_patient_id,
        practice_code
    from {{ ref('dim_person_demographics_basic') }}
    where practice_code is not null
),

with_pds as (
    select
        emis.*,
        reg.practice_code as pds_registered_practice_code,
        emis.practice_code is distinct from reg.practice_code
            as pds_registered_practice_mismatch_flag
    from emis
    left join registered_practice reg
        on emis.sk_patient_id = reg.sk_patient_id
),

with_olids as (
    select
        with_pds.*,
        dp.current_practice_code as olids_current_practice_code,
        with_pds.practice_code is distinct from dp.current_practice_code
            as olids_current_practice_mismatch_flag
    from with_pds
    left join {{ ref('dim_person_pseudo') }} pp
        on with_pds.sk_patient_id = pp.sk_patient_id
    left join {{ ref('dim_person') }} dp
        on pp.person_id = dp.person_id
)

select *
from with_olids
qualify
    case
        when count_if(not pds_registered_practice_mismatch_flag) over (partition by sk_patient_id) > 0
            then not pds_registered_practice_mismatch_flag
        when count_if(not olids_current_practice_mismatch_flag) over (partition by sk_patient_id) > 0
            then not olids_current_practice_mismatch_flag
        else true
    end