--CTE to filter down to valid 38d records
with cwt_d38_records as (
    select
        record_id
    from {{ref('int_cwt_pathway__62d_allocation_method')}} am
    
    where am.d62__allocation_method = '6 Scenarios'
)

select
    cwt.record_id,

    --Date
    date__treatment_start_date as performance__date_full,

    --Organisation
    org__accountable_investigating_trust_code as performance__trust_code,
    null as performance__site_code,

    --Performance
    '38 Day' as performance__metric,
    --Pathway Length in Days
    cast(
        case 
            when pep.pathway_entry_point != 'Upgrade'
            then
                    datediff(
                        'days',
                        date__cancer_referral_to_treatment_period_start_date,
                        date__transfer_to_treatment_date
                    )
                    - coalesce(performance__wta_first_seen_date_adjustment, 0)
            when cwt.date__consultant_upgrade_date <= cwt.date__date_first_seen
            then 
                    datediff(
                        'days',
                        date__consultant_upgrade_date,
                        date__transfer_to_treatment_date
                    )
                    - coalesce(performance__wta_first_seen_date_adjustment, 0)
            else
                    datediff(
                        'days',
                        date__consultant_upgrade_date,
                        date__transfer_to_treatment_date
                    )
        end
        as int 
    ) as performance__pathway_length_days,
    
    case 
        when performance__pathway_length_days <= 38 then 1 
        else 0
    end as performance__numerator,
    1 as performance__denominator,
    performance__denominator - performance__numerator as performance__breach

from {{ref('int_cwt_pathway__data_tidy')}} cwt

--Join to get Pathway Entry Point as used in the Pathway Length in Days logic
left join {{ref('int_cwt_pathway__pathway_entry_point')}} pep
on cwt.record_id = pep.record_id

--Filter to valid 38 Day records
where cwt.record_id in (select record_id from cwt_d38_records)