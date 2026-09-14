-- The one-row-per-record UEC models must not discard any staging record.
with counts as (
    select 'referred_to_service' as feed
        , (select count(*) from {{ ref('stg_sus_ecds_attendance_referred_to') }}) as staging_rows
        , (select count(*) from {{ ref('int_sus_uec_referred_to_service') }}) as model_rows
    union all
    select 'mental_health_legal_status'
        , (select count(*) from {{ ref('stg_sus_ecds_patient_mental_health_act_legal_status') }})
        , (select count(*) from {{ ref('int_sus_uec_mental_health_legal_status') }})
    union all
    select 'injury_alcohol_drug'
        , (select count(*) from {{ ref('stg_sus_ecds_clinical_injury_alcohol_drug_involvements') }})
        , (select count(*) from {{ ref('int_sus_uec_injury_alcohol_drug') }})
)

select *
from counts
where staging_rows <> model_rows
