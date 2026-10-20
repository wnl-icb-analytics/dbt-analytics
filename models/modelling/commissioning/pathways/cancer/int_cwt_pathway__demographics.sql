{{ 
    config(materialized='view') 
}}

select 
    cwt.record_id,
    --Demographic Information
    pmi.gender as demographics__gender,
    pmi.date_of_birth as demographics__date_of_birth,
    pmi.date_of_death as demographics__date_of_death,
    pmi.ethnicity_code as demographics__ethnictity_code,
    initcap(pmi.ethnicity) as demographics__ethnicity_name,
    pmi.ethnicity_detail as demographics__ethnicity_subcategory,    
    
    --Registered Information
    gp_all.icb_code in {{ wnl_ods_codes() }}  as registered__wnl_registered_flag,
    gp_all.sub_icb_code = {{ ncl_sub_icb() }} as registered__ncl_registered_flag,
    gp_all.sub_icb_code = {{ nwl_sub_icb() }} as registered__nwl_registered_flag,
    cwt.at_event__registered__practice_code   as registered__practice_code,
    gp_all.practice_name                      as registered__practice_name,
    gp_all.close_date                         as registered__practice_end_date,
    gp_all.pcn_code                           as registered__pcn_code,
    gp_all.pcn_name                           as registered__pcn_name,
    gp_all.sub_icb_code                       as registered__subicb_code,
    gp_all.sub_icb_name                       as registered__subicb_name,
    gp_all.icb_code                           as registered__icb_code,
    gp_all.icb_name                           as registered__icb_name,
    
    --Logic to map out of area to 'Non-NCL'
    --Note a null subicb will still have a null value as borough cannot be determined
    --This is required as practice to borough mapping does not exist outside of NCL
    case 
        when registered__wnl_registered_flag = false
        then 'Non-WNL'
        when registered__wnl_registered_flag = true
        then coalesce(gp_wnl.health_borough_name, 'Unknown')
        else null
    end                                     as registered__borough,
    
    gp_imd.imd_decile                       as registered__imd_decile,
    dict_subicb_ca.cancer_alliance_code     as registered__cancer_alliance_code,
    dict_subicb_ca.cancer_alliance_name     as registered__cancer_alliance_name,

    --Residence Information
    --Note older records use older LSOA formats which can fail when trying to map to higher geographies
    dict_lsoa_subicb.icb_ods_code in {{ wnl_ods_codes() }}  as residence__wnl_residence_flag,
    dict_lsoa_subicb.sub_icb_ods_code = {{ ncl_sub_icb() }} as residence__ncl_residence_flag,
    dict_lsoa_subicb.sub_icb_ods_code = {{ nwl_sub_icb() }} as residence__nwl_residence_flag,
    coalesce(
        dict_lsoa_versioning.lsoa21_cd, 
        cwt.at_event__residence__lsoa_code
    )                                                       as residence__lsoa_code,
    dict_lsoa_subicb.sub_icb_ods_code                       as residence__subicb_code,
    dict_lsoa_subicb.icb_ods_code                           as residence__icb_code,
    dict_lsoa_subicb.icb_name                               as residence__icb_name,
    dict_lsoa_subicb.lad_name                               as residence__borough,
    imd.imd_decile                                          as residence__imd_decile,
    
    --Logic to map out of area to 'Non-NCL'
    --Note a null subicb will still have a null value as neighbourhood cannot be determined
    --This is required as we do not have neighbour definitions for out of area
    case 
        when residence__wnl_residence_flag = false
        then 'Non-WNL'
        when residence__wnl_residence_flag = true
        then coalesce(dict_lsoa_nb.neighbourhood_name, 'Unknown')
        else null
    end                                     as residence__neighbourhood_name,
    
    dict_lsoa_ca.cancer_alliance_code       as residence__cancer_alliance_code,
    dict_lsoa_ca.cancer_alliance_name       as residence__cancer_alliance_name

from {{ ref('int_cwt_pathway__data_tidy') }} cwt

-- Demographic Joins
left join {{ ref('dim_person_demographics_basic') }} pmi
on cwt.sk_patient_id = pmi.sk_patient_id

-- Registration Joins
left join {{ ref('practice_national_all') }} gp_all
on registered__practice_code = gp_all.practice_code

left join {{ ref('practice_all') }} gp_wnl
on registered__practice_code = gp_wnl.practice_code

left join {{ ref('imd_2025_practice_national') }} gp_imd
on registered__practice_code = gp_imd.practice_code
and gp_imd.is_latest = true

left join {{ ref('stg_reference_cancer_sub_icb_to_cancer_alliance') }} dict_subicb_ca
on registered__subicb_code = dict_subicb_ca.sub_icb_code

--Residence Joins
left join {{ ref('stg_reference_lsoa2011_lsoa2021') }} dict_lsoa_versioning
on cwt.at_event__residence__lsoa_code = dict_lsoa_versioning.lsoa11_cd

left join {{ ref('stg_ukhfd_lsoa_sub_icb_plus_dim_map_scd') }} dict_lsoa_subicb
on residence__lsoa_code = dict_lsoa_subicb.lsoa_code
and dict_lsoa_subicb.is_latest = 1

left join {{ ref('imd_2025_lsoa_2021') }} imd
on residence__lsoa_code = imd.lsoa_code

left join {{ ref('stg_reference_geo_neighbourhood_lsoa') }} dict_lsoa_nb
on residence__lsoa_code = dict_lsoa_nb.lsoa_2021_code

left join {{ ref('stg_reference_cancer_lsoa_to_cancer_alliance') }} dict_lsoa_ca
on residence__lsoa_code = dict_lsoa_ca.lsoa_2021_code