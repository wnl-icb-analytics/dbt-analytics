--CTE to filter down to valid 2WW records
with cwt_valid_records as (
    select
        record_id
    from {{ref('int_cwt_pathway__data_tidy')}}
    
    where referral__priority_type_code = 3
    and referral__source_of_referral_for_outpatient_code != 17
    and date__cancer_referral_to_treatment_period_start_date is not null
    and date__date_first_seen is not null
)

select
    record_id,

    --Date
    date__date_first_seen as performance__date_full,

    --Organisation
    org__first_seen_trust_code as performance__trust_code,
    org__first_seen_site_code as performance__site_code,

    --Performance
    '2WW' as performance__metric,
    (
        cast(
            datediff(
                'days',
                date__cancer_referral_to_treatment_period_start_date,
                date__date_first_seen
            )
            - coalesce(performance__wta_first_seen_date_adjustment, 0)
            as int
        )
    ) as performance__pathway_length_days,
    case 
        when performance__pathway_length_days <= 14 then 1
        else 0
    end as performance__numerator,
    1 as performance__denominator,
    performance__denominator - performance__numerator as performance__breach

from {{ref('int_cwt_pathway__data_tidy')}}

--Filter to valid 2WW records
where record_id in (select record_id from cwt_valid_records)