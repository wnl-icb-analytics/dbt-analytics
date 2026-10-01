{{ config(materialized='table', tags=['cltcs']) }}

with inclusion_list as (
    select *
    from {{ ref('cltcs_adult_population') }}
    ),

encoding_features as(
    select il.sk_patient_id
        , il.practice_code
        , zeroifnull(wl.wl_current_total_count) as wl_total_count
        , zeroifnull(wl.wl_current_distinct_providers_count) as wl_provider_count
        , zeroifnull(opa.op_att_tot_12mo) as op_att_tot_12mo
        , zeroifnull(opa.op_spec_12mo) as op_spec_12mo
        , zeroifnull(opa.op_prov_12mo) as op_prov_12mo
        , zeroifnull(gpa.gp_att_tot_12mo) as gp_att_tot_12mo
        , zeroifnull(aea.ae_tot_12mo) as ae_tot_12mo
        , zeroifnull(apca.apc_los_12mo) as apc_los_12mo
        , zeroifnull(apca.apc_12mo) as apc_12mo
        , case when wl.same_tfc_multiple_providers_flag  = TRUE then 1 else 0 end as has_same_tfc_multiple_providers_flag_flag
    from inclusion_list il
    left join {{ref('fct_person_wl_current_count_total')}} wl
        on il.sk_patient_id = wl.sk_patient_id
    left join {{ref('fct_person_sus_op_recent')}} opa
        on il.sk_patient_id  = opa.sk_patient_id
    left join {{ref('fct_person_gp_recent')}} gpa
        on il.sk_patient_id  = gpa.sk_patient_id
    left join {{ref('fct_person_sus_uec_recent')}} aea
        on il.sk_patient_id  = aea.sk_patient_id
    left join {{ref('fct_person_sus_apc_recent')}} apca
        on il.sk_patient_id  = apca.sk_patient_id
)
select
    sk_patient_id,
    practice_code,
    (wl_total_count * wl_provider_count) + op_att_tot_12mo/3 + op_prov_12mo*2 + op_spec_12mo*2 - gp_att_tot_12mo*4 - ae_tot_12mo - apc_los_12mo + has_same_tfc_multiple_providers_flag_flag * 2 as score_coordination
from encoding_features
