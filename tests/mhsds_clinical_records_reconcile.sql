with expected as (
    select clinical_record_type, count(*) as expected_count
    from {{ ref('int_mhsds_diagnosis') }}
    group by clinical_record_type

    union all
    select 'referral_assessment', count(*) from {{ ref('stg_mhsds_referral_assessment') }}
    union all
    select 'activity_assessment', count(*) from {{ ref('stg_mhsds_activity_assessment') }}
    {% for component in ['procedure', 'finding', 'observation'] %}
    union all
    select '{{ component }}', count_if(has_{{ component }})
    from {{ ref('fct_mhsds_care_activity') }}
    {% endfor %}
)

, actual as (
    select clinical_record_type, count(*) as actual_count
    from {{ ref('fct_mhsds_clinical_record') }}
    group by clinical_record_type
)

select
    coalesce(e.clinical_record_type, a.clinical_record_type) as clinical_record_type
    , coalesce(e.expected_count, 0) as expected_count
    , coalesce(a.actual_count, 0) as actual_count
from expected as e
full outer join actual as a
    on e.clinical_record_type = a.clinical_record_type
where coalesce(e.expected_count, 0) <> coalesce(a.actual_count, 0)
