{{ 
    config(materialized='view') 
}}

--CTE For flag columns
with flags as (
    select 
        record_id,
        --Flags
        (
            referral__priority_type_code = 3 and 
            referral__cancer_referral_type_code != 16 and
            referral__source_of_referral_for_outpatient_code != 17 and
            date__consultant_upgrade_date is null
        ) as pathway_entry_point_flag__usc,
    
        (
            referral__priority_type_code = 3 and 
            referral__cancer_referral_type_code = 16 and
            referral__source_of_referral_for_outpatient_code != 17 and
            date__consultant_upgrade_date is null
        ) as pathway_entry_point_flag__breast_symptomatic,
    
        (
            referral__priority_type_code = 2 and 
            referral__source_of_referral_for_outpatient_code = 17
        ) as pathway_entry_point_flag__screening,
    
        (
            referral__source_of_referral_for_outpatient_code != 17 and
            date__consultant_upgrade_date is not null
        ) as pathway_entry_point_flag__upgrade,

        (
            treatment__event_type_code is not null and
            date__treatment_start_date is not null
        ) as pathway_entry_point_flag__treatment
        
    from {{ref('int_cwt_pathway__data_tidy')}}
)

select
    record_id,
    case
        when pathway_entry_point_flag__usc then 'USC'
        when pathway_entry_point_flag__breast_symptomatic then 'Breast Symptomatic'
        when pathway_entry_point_flag__screening then 'Screening'
        when pathway_entry_point_flag__upgrade then 'Upgrade'
        when pathway_entry_point_flag__treatment then 'Treatment'
        else 'Unknown'
    end as pathway_entry_point
    
from flags