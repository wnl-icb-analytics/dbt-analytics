with unique_person_ids as (
    select sk_patient_id, person_id
    from {{ref('dim_person_pseudo')}}
    qualify row_number() over (
        partition by sk_patient_id
        order by person_id desc
    ) = 1
)

select dp.sk_patient_id
    , upi.person_id
    , zeroifnull(hu.AE_ILL_12MO) as AE_ILL_12MO 
	,zeroifnull(hu.AE_ILL_3MO) as AE_ILL_3MO 
	,zeroifnull(hu.AE_ILL_1MO) as AE_ILL_1MO 
	,zeroifnull(hu.AE_TOT_12MO) as AE_TOT_12MO 
	,zeroifnull(hu.AE_INJ_12MO) as AE_INJ_12MO 
	,zeroifnull(hu.AE_T1_12MO) as AE_T1_12MO 
	,zeroifnull(ha.APC_3MO) as APC_3MO 
	,zeroifnull(ha.APC_1MO) as APC_1MO 
	,zeroifnull(ha.APC_12MO) as APC_12MO 
	,zeroifnull(ha.APC_LOS_12MO) as APC_LOS_12MO
	,zeroifnull(ha.APC_NEL_12MO) as APC_NEL_12MO 
	,zeroifnull(ho.OP_ATT_TOT_12MO) as OP_ATT_TOT_12MO  
	,zeroifnull(ho.OP_ATT_TOT_3MO) as OP_ATT_TOT_3MO  
	,zeroifnull(ho.OP_ATT_TOT_1MO) as OP_ATT_TOT_1MO  
	,zeroifnull(ho.OP_ATT_FIRST_12MO) as OP_ATT_FIRST_12MO 
	,zeroifnull(ho.OP_APP_TOT_12MO) as OP_APP_TOT_12MO  
	,zeroifnull(ho.OP_SPEC_12MO) as OP_SPEC_12MO  
	,zeroifnull(ho.OP_PROV_12MO) as OP_PROV_12MO  
	,zeroifnull(ho.OP_NUM_SPEC_2_PROV_12MO) as OP_NUM_SPEC_2_PROV_12MO
	,zeroifnull(hg.GP_ATT_TOT_12MO) as GP_ATT_TOT_12MO
	,zeroifnull(hg.GP_ATT_TOT_3MO) as GP_ATT_TOT_3MO 
	,zeroifnull(hg.GP_ATT_TOT_1MO) as GP_ATT_TOT_1MO 
	,zeroifnull(hg.GP_APP_TOT_12MO) as GP_APP_TOT_12MO 
	,zeroifnull(hg.GP_DNA_TOT_12MO) as GP_DNA_TOT_12MO
from {{ref('dim_person_demographics_basic')}} as dp
left join unique_person_ids as upi on dp.sk_patient_id = upi.sk_patient_id
left join {{ref('fct_person_gp_recent')}} as hg on dp.sk_patient_id = hg.sk_patient_id
left join {{ref('fct_person_sus_uec_recent')}} as hu on dp.sk_patient_id = hu.sk_patient_id
left join {{ref('fct_person_sus_op_recent')}} as ho on dp.sk_patient_id = ho.sk_patient_id
left join {{ref('fct_person_sus_apc_recent')}} as ha on dp.sk_patient_id = ha.sk_patient_id
