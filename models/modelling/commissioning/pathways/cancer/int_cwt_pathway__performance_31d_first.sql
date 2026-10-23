--CTE to filter down to valid 31day first treatment records
with cwt_31f_records as (
    select
        record_id
    from {{ref('int_cwt_pathway__data_tidy')}}
    
    where treatment__event_type_code in ('01', '07', '12')
    and treatment__modality_code <> '98'
    and treatment__primary_icd10_code is not null
    and date__cancer_treatment_period_date is not null
    and date__treatment_start_date is not null
)

select
    record_id,

    --Date
    date__treatment_start_date as performance__date_full,

    --Organisation
    org__accountable_treating_trust_code as performance__trust_code,
    org__accountable_treating_site_code as performance__site_code,

    --Performance
    '31 Day' as performance__metric,
    (
        cast(
            datediff(
                'days',
                date__cancer_treatment_period_date,
                 date__treatment_start_date
            )
            - coalesce(performance__wta_treatment_start_date_adjustment, 0)
            as int
        )
    ) as performance__pathway_length_days,
    case 
        when performance__pathway_length_days <= 31 then 1 
        else 0
    end as performance__numerator,
    1 as performance__denominator,
    performance__denominator - performance__numerator as performance__breach

from {{ref('int_cwt_pathway__data_tidy')}}

--Filter to valid 31day first records
where record_id in (select record_id from cwt_31f_records)