{% test nice_indicator_history_semantic_counts(model) %}

-- Per indicator and month, denominator_count must equal the fact's rows, whose
-- person_id, indicator_id and reporting_date key is tested upstream.
select 'indicator_' || coalesce(s.indicator_id, d.indicator_id) || '_' || coalesce(s.reporting_date, d.reporting_date) as entity,
    s.denominator_count as semantic_count, d.row_count as domain_count
from semantic_view(
    {{ model }} metrics indicators.denominator_count dimensions indicators.indicator_id, indicators.reporting_date
) as s
full outer join (
    select indicator_id, reporting_date, count(*) as row_count
    from {{ ref('fct_person_nice_indicator_status_by_month') }}
    group by indicator_id, reporting_date
) as d on s.indicator_id = d.indicator_id and s.reporting_date = d.reporting_date
where s.denominator_count <> d.row_count
    or s.indicator_id is null
    or d.indicator_id is null

union all

-- Splitting by a person-month dimension must keep every row, so the
-- person and month-end join drops no one. The latest person month is
-- recalculated daily while this fact is rebuilt monthly, so it is skipped.
select 'person_month_join_' || d.indicator_id || '_' || d.reporting_date as entity,
    coalesce(s.semantic_count, 0) as semantic_count, d.row_count as domain_count
from (
    select indicator_id, reporting_date, count(*) as row_count
    from {{ ref('fct_person_nice_indicator_status_by_month') }}
    where reporting_date < (select max(analysis_month) from {{ ref('person_month_analysis_base') }})
    group by indicator_id, reporting_date
) as d
left join (
    select indicator_id, reporting_date, sum(denominator_count) as semantic_count
    from semantic_view(
        {{ model }} metrics indicators.denominator_count
        dimensions indicators.indicator_id, indicators.reporting_date, person_months.gender
    )
    where gender is not null
    group by indicator_id, reporting_date
) as s on s.indicator_id = d.indicator_id and s.reporting_date = d.reporting_date
where coalesce(s.semantic_count, 0) <> d.row_count

{% endtest %}
