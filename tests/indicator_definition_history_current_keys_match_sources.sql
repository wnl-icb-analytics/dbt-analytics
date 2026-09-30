{% set definitions = ref('def_indicator') %}
{% set usage = ref('def_indicator_usage') %}

with definition_history as (
    select indicator_id, count(*) as current_row_count
    from {{ definitions.database }}.{{ definitions.schema }}.def_indicator_history
    where is_current
    group by indicator_id
),

invalid_definitions as (
    select
        'definition' as history_type,
        coalesce(source_rows.indicator_id, history_rows.indicator_id) as indicator_id,
        cast(null as varchar) as usage_context,
        coalesce(history_rows.current_row_count, 0) as current_row_count
    from {{ ref('def_indicator') }} source_rows
    full outer join definition_history history_rows
        on source_rows.indicator_id = history_rows.indicator_id
    where source_rows.indicator_id is null
        or coalesce(history_rows.current_row_count, 0) != 1
),

usage_history as (
    select indicator_id, usage_context, count(*) as current_row_count
    from {{ usage.database }}.{{ usage.schema }}.def_indicator_usage_history
    where is_current
    group by indicator_id, usage_context
),

invalid_usage as (
    select
        'usage' as history_type,
        coalesce(source_rows.indicator_id, history_rows.indicator_id) as indicator_id,
        coalesce(source_rows.usage_context, history_rows.usage_context) as usage_context,
        coalesce(history_rows.current_row_count, 0) as current_row_count
    from {{ ref('def_indicator_usage') }} source_rows
    full outer join usage_history history_rows
        on source_rows.indicator_id = history_rows.indicator_id
        and source_rows.usage_context = history_rows.usage_context
    where source_rows.indicator_id is null
        or coalesce(history_rows.current_row_count, 0) != 1
)

select * from invalid_definitions
union all
select * from invalid_usage
