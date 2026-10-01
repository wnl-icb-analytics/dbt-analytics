{% test nice_indicator_semantic_counts(model) %}

-- Every metric counts distinct people, so duplicated rows cannot change results.
-- Per indicator, denominator_count must equal the fact's rows, whose
-- person_id and indicator_id key is tested upstream.
select 'indicators' as entity, s.semantic_count, d.row_count as domain_count
from (
    select coalesce(sum(denominator_count), 0) as semantic_count
    from semantic_view({{ model }} metrics indicators.denominator_count dimensions indicators.indicator_id)
) as s
cross join (select count(*) as row_count from {{ ref('fct_person_nice_indicator_status') }}) as d
where s.semantic_count <> d.row_count

union all

select 'indicator_' || coalesce(s.indicator_id, d.indicator_id) as entity,
    s.denominator_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics indicators.denominator_count dimensions indicators.indicator_id) as s
full outer join (
    select indicator_id, count(*) as row_count
    from {{ ref('fct_person_nice_indicator_status') }}
    group by indicator_id
) as d on s.indicator_id = d.indicator_id
where s.denominator_count <> d.row_count
    or s.indicator_id is null
    or d.indicator_id is null

union all

-- Keep indicator groups separate when counting people by demographics.
select 'indicators_by_demographics' as entity, s.semantic_count, d.row_count as domain_count
from (
    select coalesce(sum(denominator_count), 0) as semantic_count
    from semantic_view(
        {{ model }} metrics indicators.denominator_count dimensions indicators.indicator_id, demographics.gender
    )
) as s
cross join (select count(*) as row_count from {{ ref('fct_person_nice_indicator_status') }}) as d
where s.semantic_count <> d.row_count

{% endtest %}
