-- Fails when a configured QAdmissions index date has no rows in
-- qadmissions_input_features_history. The model inner-joins its index dates to
-- int_segmentation_person_month_spine, so a date that isn't a month-end inside
-- the spine's window, or a spine that hasn't been rebuilt since a new
-- month-end, silently produces no rows for that date while every other test
-- passes.
--
-- Returns one row per index date with no rows. Index dates come from
-- qadmissions_history_index_dates(), the list the model reads.
with configured_dates as (
{%- for d in qadmissions_history_index_dates() %}
    {% if not loop.first %}union all{% endif %}
    select '{{ d }}'::date as end_date
{%- endfor %}
),

built_dates as (
    select distinct end_date
    from {{ ref('qadmissions_input_features_history') }}
)

select c.end_date
from configured_dates as c
left join built_dates as b
    on c.end_date = b.end_date
where b.end_date is null
