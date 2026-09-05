-- Reference examples are public metadata; clinical failures stay aggregate-only.
with expected as (
    select column1 as concept_code, column2 as response_code,
        column3 as response_description, column4 as is_non_score_response
    from values
        ('987191000000101', '888', 'Not known', true),
        ('987191000000101', '999', 'Missing data', true),
        ('985981000000108', 'A', 'Phobic', false),
        ('979831000000108', 'B', 'Anxiety', false),
        ('763264000', '9', 'Terminally ill', false),
        ('747881000000109', 'NA', 'Not Applicable', true)
)
select 'published_response_meaning' as failure_reason,
    e.concept_code, e.response_code,
    e.response_description as expected_description,
    r.response_description as actual_description,
    e.is_non_score_response as expected_non_score,
    r.is_non_score_response as actual_non_score,
    1 as failures
from expected as e
left join {{ ref('mhsds_assessment_response') }} as r
    on e.concept_code = r.concept_code and e.response_code = r.response_code
where e.response_description is distinct from r.response_description
    or e.is_non_score_response is distinct from r.is_non_score_response

union all

select 'non_score_published_as_score', null, null, null, null, null, null, count(*)
from {{ ref('fct_mhsds_clinical_record') }}
where is_assessment_response_non_score and assessment_score_numeric is not null
having count(*) > 0

union all

select 'ambiguous_numeric_response_key', concept_code, numeric_response_value::varchar,
    null, null, null, null, count(*)
from {{ ref('mhsds_assessment_response') }}
where numeric_response_value is not null
group by concept_code, numeric_response_value
having count(*) > 1
