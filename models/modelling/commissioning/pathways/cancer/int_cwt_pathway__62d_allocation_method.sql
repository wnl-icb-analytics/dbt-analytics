--CTE to get a list of valid records
with valid_records as (
    select 
        cwt.record_id
        
    from {{ref('int_cwt_pathway__data_tidy')}} cwt
    
    left join {{ref('int_cwt_pathway__pathway_entry_point')}} pep
    on cwt.record_id = pep.record_id
    
    --Valid 62 Day record conditions--------------------------
    -- First Treatment
    where cwt.treatment__event_type_code in ('01', '07', '12')
    -- Did not decline treatment
    and cwt.treatment__modality_code != '98'
    -- Has a diagnosis
    and cwt.treatment__primary_icd10_code is not null
    -- Has a Cancer Treatment Period Start Date and Treatment Start Date
    and cwt.date__cancer_treatment_period_date is not null
    and cwt.date__treatment_start_date is not null
    -- Either has a First Seen Organisation or is a Consultant Upgrade
    and (
        cwt.org__first_seen_site_code is not null or
        pep.pathway_entry_point = 'Upgrade'
    )
)

select
    cwt.record_id,
    cwt.date__treatment_start_date as performance__date_full,
    
    --Accountable organisations
    coalesce(
        cwt.org__accountable_investigating_trust_code_preset,
        cwt.org__consultant_upgrade_trust_code,
        cwt.org__first_seen_trust_code
    ) as d62__investigating_trust_code,
    cwt.org__accountable_treating_trust_code as d62__treating_trust_code,
    cwt.org__accountable_treating_site_code as d62__treating_site_code,

    --Allocation method
    case 
        when d62__investigating_trust_code = d62__treating_trust_code
            then 'Solo'
        when cwt.org__accountable_investigating_trust_code_preset is null
            then '50-50'
        else '6 Scenarios'
    end as d62__allocation_method,

    --Pathway length in days
    case 
        when pep.pathway_entry_point != 'Upgrade'
        then
            cast(
                datediff(
                    'days',
                    date__cancer_referral_to_treatment_period_start_date,
                    date__treatment_start_date
                )
                - coalesce(performance__wta_first_seen_date_adjustment, 0)
                - coalesce(performance__wta_treatment_start_date_adjustment, 0)
                as int
            )
        when cwt.date__consultant_upgrade_date <= cwt.date__date_first_seen
        then 
            cast(
                datediff(
                    'days',
                    date__consultant_upgrade_date,
                    date__treatment_start_date
                )
                - coalesce(performance__wta_first_seen_date_adjustment, 0)
                - coalesce(performance__wta_treatment_start_date_adjustment, 0)
                as int
            )
        else
            cast(
                datediff(
                    'days',
                    date__consultant_upgrade_date,
                    date__treatment_start_date
                )
                - coalesce(performance__wta_treatment_start_date_adjustment, 0)
                as int
            )
    end as performance__pathway_length_days,
    
        case 
            when performance__pathway_length_days <= 62 then 01 
            else 0
        end as performance__numerator_base,
        1 as performance__denominator_base
    
from {{ref('int_cwt_pathway__data_tidy')}} cwt

--Join to get valid records
inner join valid_records vr
on cwt.record_id = vr.record_id

--Join to get pathway entry point
inner join {{ref('int_cwt_pathway__pathway_entry_point')}} pep
on cwt.record_id = pep.record_id