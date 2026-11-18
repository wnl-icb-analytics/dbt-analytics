--CTE to filter down to valid records with a completed FDS pathway
with cwt_valid_records as (
    select
        record_id
    from {{ref('int_cwt_pathway__data_tidy')}}
    
    where diagnosis__fds_end_reason_code in ('01', '02')
    --and pep.pathway_entry_point = 'USC'
)

select
    cwt.record_id,

    --Date
    date__fds_pathway_end_date as performance__date_full,

    --Organisation
    org__faster_diagnosis_pathway_end_trust_code as performance__trust_code,
    org__faster_diagnosis_pathway_end_site_code as performance__site_code,

    --Performance
    'Conversion' as performance__metric,
    null as performance__pathway_length_days,
    case
        when diagnosis__fds_end_reason_code = '01'
        then 1
        
        when diagnosis__fds_end_reason_code is not null 
        then 0
        
        else null
    end as performance__numerator,
    1 as performance__denominator,
    null as performance__breach

from {{ref('int_cwt_pathway__data_tidy')}} cwt

left join {{ref('int_cwt_pathway__pathway_entry_point')}} pep
on cwt.record_id = pep.record_id

--Filter to valid 2WW records
where cwt.record_id in (select record_id from cwt_valid_records)