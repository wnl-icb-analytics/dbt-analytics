--For 2WW, FDS, 62 Day et al: Remove records with no clear pathway entry point
select cwt.* 
from (
    select * from {{ ref('int_cwt_pathway__performance_62d') }}
    
    union all by name
    
    select * exclude (performance__numerator, performance__denominator),
    performance__numerator, performance__denominator
    from {{ ref('int_cwt_pathway__performance_2ww') }}
    
    union all by name
    
    select * exclude (performance__numerator, performance__denominator),
    performance__numerator, performance__denominator
    from {{ ref('int_cwt_pathway__performance_fds') }}
    
    union all by name
    
    select * exclude (performance__numerator, performance__denominator),
    performance__numerator, performance__denominator
    from {{ ref('int_cwt_pathway__performance_38d') }}
    
    union all by name
    
    select * exclude (performance__numerator, performance__denominator),
    performance__numerator, performance__denominator
    from {{ ref('int_cwt_pathway__performance_24d') }}
) cwt

left join {{ ref('int_cwt_pathway__pathway_entry_point') }} pep
on cwt.record_id = pep.record_id

where pep.pathway_entry_point in ('USC', 'Breast Symptomatic', 'Upgrade', 'Screening')

union all by name 

--31 Day Records
select * exclude (performance__numerator, performance__denominator),
performance__numerator, performance__denominator
from {{ ref('int_cwt_pathway__performance_31d_first') }}

union all by name

select * exclude (performance__numerator, performance__denominator),
performance__numerator, performance__denominator
from {{ ref('int_cwt_pathway__performance_31d_subsequent') }}

--Conversion Rate
union all by name

select * exclude (performance__numerator, performance__denominator),
performance__numerator, performance__denominator
from {{ ref('int_cwt_pathway__performance_conversion') }}