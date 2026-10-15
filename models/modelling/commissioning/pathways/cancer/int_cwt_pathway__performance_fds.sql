--CTE to calculate the FDS pathway length early since it is used in the definition
with cwt_fds_pathway_length as (
    select 
    record_id,
    coalesce(
        least(
            date__cancer_treatment_period_date, 
            date__fds_pathway_end_date
        ),
        date__fds_pathway_end_date
    ) as adjusted_fds_pathway_end_date,
    (
        cast(
            datediff(
                'days',
                date__cancer_referral_to_treatment_period_start_date,
                adjusted_fds_pathway_end_date
            )
            - coalesce(performance__wta_first_seen_date_adjustment, 0)
            as int
        )
    ) as performance__pathway_length_days

    from {{ref('int_cwt_pathway__data_tidy')}}
),

--CTE to filter down to valid FDS records
cwt_valid_records as (
    select
        cwt.record_id

    from {{ref('int_cwt_pathway__data_tidy')}} cwt
        
    left join cwt_fds_pathway_length on
    cwt.record_id = cwt_fds_pathway_length.record_id
    
    where 
    --Either the pathway has a valid end code or the patient died on the pathway after 28 days elapsed
    (    
        diagnosis__fds_end_reason_code in ('01', '02', '04') or
        (
            diagnosis__fds_end_reason_code = '03' and
            diagnosis__fds_end_reason_exclusion_code = '01' and 
            performance__pathway_length_days > 28
        )
    )
    and date__cancer_referral_to_treatment_period_start_date is not null
    and date__fds_pathway_end_date is not null
)

select
    cwt.record_id,
    
    --Date
    date__fds_pathway_end_date as performance__date_full,
    
    --Organisation
    org__faster_diagnosis_pathway_end_trust_code as performance__trust_code,
    org__faster_diagnosis_pathway_end_site_code as performance__site_code,

    --Performance
    'FDS' as performance__metric,
    performance__pathway_length_days,
    case 
        when performance__pathway_length_days <= 28 then 1 
        else 0
    end as performance__numerator,
    1 as performance__denominator,
    performance__denominator - performance__numerator as performance__breach 

from {{ref('int_cwt_pathway__data_tidy')}} cwt

left join cwt_fds_pathway_length on
cwt.record_id = cwt_fds_pathway_length.record_id

--Filter to valid records
where cwt.record_id in (select record_id from cwt_valid_records)