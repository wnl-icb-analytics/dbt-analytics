-- The example codes are public reference metadata, never submitted values.
with expected as (
    select column1 as concept_code, column2 as response_code, column3 as description,
        column4 as is_non_score
    from values
        ('821551000000108', '9', 'Missing value', true),
        ('821561000000106', '9', 'Missing value', true),
        ('821581000000102', '9', 'Missing value', true),
        ('821591000000100', '9', 'Missing value', true),
        ('821611000000108', '9', 'Missing value', true),
        ('763264000', '9', 'Terminally ill', false)
)
select 'reference_response' as failure_reason, e.concept_code, e.response_code, null::varchar as source_record_id
from expected as e
left join {{ ref('csds_assessment_response') }} as r
    on e.concept_code = r.concept_code and e.response_code = r.response_code
where e.description is distinct from r.response_description
    or e.is_non_score is distinct from r.is_non_score_response
union all
select 'non_score_published_as_score', null, null, source_record_id
from {{ ref('fct_csds_clinical_record') }}
where is_assessment_response_non_score and assessment_score_numeric is not null
union all
select 'unsupported_index_range', concept_code, null, null::varchar
from {{ ref('csds_assessment_scale') }}
where concept_code = '736534008' and numeric_range_count <> 0
union all
select 'ambiguous_numeric_response', concept_code, numeric_response_value::varchar, null::varchar
from {{ ref('csds_assessment_response') }}
where numeric_response_value is not null
group by concept_code, numeric_response_value
having count(*) > 1
