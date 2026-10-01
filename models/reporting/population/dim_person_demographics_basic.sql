{{
    config(
        tags=['daily']
    )
}}

{# WNL ICB codes as they appear in MatrixView stp_code: legacy QMJ/QRV plus the
   merged Z9B2Z (defensive - NHSE may flip practices to the merged code). #}
{%
    set in_area_icbs = "('QMJ', 'QRV', 'Z9B2Z')"
%}

select
    --Record information
        pmi.sk_patient_id,

        --Person information
        pmi.gender_code,
        dict_g.gender,
        pmi.date_of_birth,
        pmi.date_of_death,
        
        --Ethnicity information
        eth.*,

        --Residence information
        pmi.flag_current_resident,
        pmi.record_residence_start_date,
        geo.lsoa_2021_code as residence_lsoa_2021_code,
        geo.lsoa_2021_name as residence_lsoa_2021_name,
        geo.ward_2025_code as residence_ward_2025_code,
        geo.ward_2025_name as residence_ward_2025_name,
        geo.local_authority_2025_name as residence_borough,
        geo.icb_code as residence_icb_code,
        geo.icb_name as residence_icb_name,
        geo.resident_flag as residence_icb_group,
        nb_res_code.neighbourhood_code as residence_neighbourhood_code,
        nb_res.neighbourhood_name as residence_neighbourhood_name,
        imd.imd25_decile as residence_imd_decile,

        --Registered information
        pmi.flag_current_registered,
        pmi.record_registered_start_date,
        dict_gp.organisation_code as practice_code,
        dict_gp.organisation_name as practice_name,
        dict_pcn.network_code as pcn_code,
        dict_pcn.network_name as pcn_name,
        dict_pcn.stp_code as icb_code,
        dict_pcn.stp_name as icb_name,
        -- Registered borough / sub-ICB via ODS organisation relationships
        -- (int_organisation_borough_mapping): one borough per practice, contractual
        -- (how the practice is commissioned), WNL-wide. Not geographic.
        coalesce(
            org_bor.borough_registered,
            case when icb_code not in {{in_area_icbs}} then 'Out of area' end,
            'Unknown'
        ) as registered_borough,
        org_bor.sub_icb_code as registered_sub_icb_code,
        org_bor.sub_icb_name as registered_sub_icb_name,
        nb_reg.neighbourhood_code as registered_neighbourhood_code,
        case
            when (icb_code not in {{in_area_icbs}} and nb_reg.neighbourhood_code is null) then 'Out of area Neighbourhood'
            else coalesce(nb_reg.neighbourhood_name, 'Unknown')
        end as registered_neighbourhood_name,
        --------------------------------------

        --Language information
        dict_pl.preferred_language,
        dict_ir.interpreter_required

from {{ref('int_person_pmi_combined')}} pmi

left join {{ref('stg_dictionary_dbo_gender')}} as dict_g
on pmi.gender_code = dict_g.gender_code

left join {{ref('stg_reference_lookup_ncl_preferred_language')}} as dict_pl
on pmi.preferred_language_code = dict_pl.code

left join {{ref('stg_reference_lookup_ncl_interpreter_required')}} as dict_ir
on pmi.interpreter_required = dict_ir.interpreter_required

left join {{ref('stg_reference_lookup_ncl_lsoa_2021_ward_2025_local_authority_2025')}} geo
on pmi.lsoa21_code = geo.lsoa_2021_code

left join {{ref('stg_reference_wnl_neighbourhood_lsoa')}} nb_res
on pmi.lsoa21_code = nb_res.lsoa_2021_code

-- NCL residence neighbourhood code (WNL source is name-only; NWL has no code)
left join {{ref('stg_reference_lookup_ncl_ncl_neighbourhood_lsoa_2021')}} nb_res_code
on pmi.lsoa21_code = nb_res_code.lsoa_2021_code

left join {{ref('stg_reference_lookup_ncl_imd_2025')}} imd
on pmi.lsoa21_code = imd.lsoa_code_2021

left join {{ref('stg_dictionary_dbo_organisation')}} dict_gp
on pmi.practice_code = dict_gp.organisation_code
and dict_gp.end_date is null

left join {{ref('stg_dictionary_dbo_organisationmatrixpracticeview')}} as dict_pcn
on dict_gp.sk_organisation_id = dict_pcn.sk_organisation_id_practice

left join (
        -- ODS-derived borough/sub-ICB, one deterministic row per practice
        select practice_code, borough_registered, sub_icb_code, sub_icb_name
        from {{ref('int_organisation_borough_mapping')}}
        qualify row_number() over (
            partition by practice_code
            order by borough_registered, sub_icb_code, sub_icb_name
        ) = 1
) org_bor
on pmi.practice_code = org_bor.practice_code

left join {{ref('stg_reference_wnl_gp_practice_neighbourhood')}} nb_reg
on pmi.practice_code = nb_reg.practice_code

left join (
    select distinct
        bk_ethnicity_code as ethnicity_code,
        ethnicity_desc,
        split_part(ethnicity_desc, ':', 0) as ethnicity,
        ethnicity_desc2 as ethnicity_detail
    from {{ref('stg_dictionary_dbo_ethnicity')}}
) eth
on pmi.ethnicity_code = eth.ethnicity_code