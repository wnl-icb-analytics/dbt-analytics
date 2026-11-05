{{
    config(
        materialized = 'table',
        tags = ['sdl', 'slam']
    )
}}

-- Latest-submission lookup for the 4 SLAM feeds: one row per selected file x
-- provider x file site x FY x activity month. The stg_*_latest views join it
-- back to rows. Rebuilt fully each run; the stg_ls* tables stay pure append.
--
-- Selection, per provider, file site token, activity FY and month:
--   * A component is one (site, recipient) stream: parallel contracts sent to
--     different recipients are kept side by side.
--   * Files restate whole periods (national ACM guidance), so within a
--     component the latest upload wins: the latest file plus same-component
--     files loaded in the 12 hours before it (complementary parts).
--   * Where a slice keeps several files, the stg_*_latest views keep, per
--     commissioner x POD x treatment function, only the latest file's rows.
--     This also absorbs streams consolidated under one recipient.
--   * Ordering: files whose majority FY is the activity FY first, then ISL
--     load time (registry created_datetime), then file and batch id. Version
--     numbers and submitted periods do not order resubmissions reliably.
--   * A component is retired when a later component restates it under a new
--     recipient code: the later one carries >= 80% of the older component's
--     content weight under the same commissioner codes and their month totals
--     are within 10%, or the recode is listed in recipient_lineage. Content
--     weight is absolute actual price for LSACM and row count for the
--     patient-level feeds; month totals are signed actual price for LSACM.

{% set feeds = [
    ('LSACM', 'stg_lsacm'),
    ('LSPLCM', 'stg_lsplcm'),
    ('LSDrPLCM', 'stg_lsdrplcm'),
    ('LSDePLCM', 'stg_lsdeplcm'),
] %}
{% set component = 'feed, dv_provider_code, site_code, recipient_code, dv_financial_year, dv_financial_month' %}
{% set file_order = 'is_in_file_financial_year desc, submission_loaded_at desc nulls last, meta_file_id desc, meta_batch_id desc' %}

{% macro same_component(a, b) %}
        {{ a }}.feed = {{ b }}.feed
       and equal_null({{ a }}.dv_provider_code, {{ b }}.dv_provider_code)
       and equal_null({{ a }}.site_code, {{ b }}.site_code)
       and equal_null({{ a }}.recipient_code, {{ b }}.recipient_code)
       and equal_null({{ a }}.dv_financial_year, {{ b }}.dv_financial_year)
       and equal_null({{ a }}.dv_financial_month, {{ b }}.dv_financial_month)
{% endmacro %}

{% macro same_slice(a, b) %}
        {{ a }}.feed = {{ b }}.feed
       and equal_null({{ a }}.dv_provider_code, {{ b }}.dv_provider_code)
       and equal_null({{ a }}.site_code, {{ b }}.site_code)
       and equal_null({{ a }}.dv_financial_year, {{ b }}.dv_financial_year)
       and equal_null({{ a }}.dv_financial_month, {{ b }}.dv_financial_month)
{% endmacro %}

with recipient_lineage as (
    -- Reviewed recipient recodes the weight test cannot detect because the
    -- commissioner codes changed at the same time.
    select *
    from (values
        -- RWH 2024/25: Flex to 93C restated by Freeze to 06K (commissioner 14E -> 13R)
        ('RWH00', '93C', '06K', '202425')
    ) as v (site_code, from_recipient_code, to_recipient_code, dv_financial_year)
),

-- One row per file x month x commissioner x POD x treatment function: the
-- content group the stg_*_latest views resolve overlaps on.
content_groups as (
    {% for feed, model in feeds %}
    select
        '{{ feed }}'                            as feed,
        meta_file_id,
        meta_batch_id,
        dv_provider_code,
        meta_provider_code                      as site_code,
        meta_recipient_code                     as recipient_code,
        dv_financial_year,
        dv_financial_month,
        commissioner_code,
        point_of_delivery_code,
        activity_treatment_function_code,
        submission_loaded_at,
        submission_file_name,
        count(*)                                as row_count,
        {% if feed == 'LSACM' %}
        coalesce(sum(abs(dv_actual_price)), 0)  as content_weight,
        coalesce(sum(dv_actual_price), 0)       as month_total
        {% else %}
        count(*)                                as content_weight,
        count(*)                                as month_total
        {% endif %}
    from {{ ref(model) }}
    group by all
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

-- Majority FY of each file's rows. Rows restating another FY rank below that
-- FY's own files.
file_fy as (
    select feed, meta_file_id, meta_batch_id, dv_financial_year as file_financial_year
    from content_groups
    group by all
    qualify row_number() over (
        partition by feed, meta_file_id, meta_batch_id
        order by sum(row_count) desc, dv_financial_year desc nulls last
    ) = 1
),

file_months as (
    select
        c.feed,
        c.meta_file_id,
        c.meta_batch_id,
        c.dv_provider_code,
        c.site_code,
        c.recipient_code,
        c.dv_financial_year,
        c.dv_financial_month,
        c.submission_loaded_at,
        c.submission_file_name,
        equal_null(f.file_financial_year, c.dv_financial_year) as is_in_file_financial_year
    from content_groups as c
    inner join file_fy as f
        on f.feed = c.feed
       and f.meta_file_id = c.meta_file_id
       and equal_null(f.meta_batch_id, c.meta_batch_id)
    group by all
),

ranked_file_months as (
    select
        *,
        row_number() over (partition by {{ component }} order by {{ file_order }}) as component_rank,
        first_value(submission_loaded_at) over (partition by {{ component }} order by {{ file_order }})
                                                as component_latest_loaded_at,
        first_value(is_in_file_financial_year) over (partition by {{ component }} order by {{ file_order }})
                                                as component_latest_in_year
    from file_months
),

-- Latest upload per component: the latest file plus same-component files
-- loaded in the 12 hours before it.
event_files as (
    select *
    from ranked_file_months
    where component_rank = 1
       or (
            is_in_file_financial_year = component_latest_in_year
            and submission_loaded_at >= dateadd(hour, -12, component_latest_loaded_at)
       )
),

-- Content of each component's latest upload after resolving overlaps the way
-- the views do (latest file per content group).
event_groups as (
    select
        c.feed, c.dv_provider_code, c.site_code, c.recipient_code, c.dv_financial_year, c.dv_financial_month,
        c.commissioner_code, c.content_weight, c.month_total
    from event_files as e
    inner join content_groups as c
        on {{ same_component('c', 'e') }}
       and c.meta_file_id = e.meta_file_id
       and equal_null(c.meta_batch_id, e.meta_batch_id)
    qualify row_number() over (
        partition by c.feed, c.dv_provider_code, c.site_code, c.recipient_code, c.dv_financial_year,
                     c.dv_financial_month, c.commissioner_code, c.point_of_delivery_code,
                     c.activity_treatment_function_code
        order by e.is_in_file_financial_year desc, e.submission_loaded_at desc nulls last,
                 e.meta_file_id desc, e.meta_batch_id desc
    ) = 1
),

event_commissioners as (
    select {{ component }}, commissioner_code,
           sum(content_weight) as content_weight, sum(month_total) as month_total
    from event_groups
    group by all
),

components as (
    select
        e.feed, e.dv_provider_code, e.site_code, e.recipient_code, e.dv_financial_year, e.dv_financial_month,
        boolor_agg(e.component_latest_in_year)  as is_in_file_financial_year,
        max(e.component_latest_loaded_at)       as latest_loaded_at,
        max(w.content_weight)                   as content_weight,
        max(w.month_total)                      as month_total
    from event_files as e
    left join (
        select {{ component }}, sum(content_weight) as content_weight, sum(month_total) as month_total
        from event_commissioners
        group by all
    ) as w
        on {{ same_component('w', 'e') }}
    group by all
),

-- Later components at the same site and month, and the share of the older
-- component's content weight under commissioner codes the later one carries.
component_successors as (
    select
        older.feed, older.dv_provider_code, older.site_code, older.dv_financial_year, older.dv_financial_month,
        older.recipient_code                    as older_recipient_code,
        later.recipient_code                    as later_recipient_code,
        older.content_weight                    as older_content_weight,
        older.month_total                       as older_month_total,
        later.month_total                       as later_month_total
    from components as older
    inner join components as later
        on {{ same_slice('later', 'older') }}
       and not equal_null(later.recipient_code, older.recipient_code)
       and (
            (later.is_in_file_financial_year and not older.is_in_file_financial_year)
            or (later.is_in_file_financial_year = older.is_in_file_financial_year
                and later.latest_loaded_at > older.latest_loaded_at)
       )
),

recode_candidates as (
    select
        s.*,
        sum(iff(lc.feed is not null, oc.content_weight, 0)) / nullif(s.older_content_weight, 0)
                                                as shared_commissioner_share
    from component_successors as s
    inner join event_commissioners as oc
        on oc.feed = s.feed
       and equal_null(oc.dv_provider_code, s.dv_provider_code)
       and equal_null(oc.site_code, s.site_code)
       and equal_null(oc.recipient_code, s.older_recipient_code)
       and equal_null(oc.dv_financial_year, s.dv_financial_year)
       and equal_null(oc.dv_financial_month, s.dv_financial_month)
    left join event_commissioners as lc
        on lc.feed = s.feed
       and equal_null(lc.dv_provider_code, s.dv_provider_code)
       and equal_null(lc.site_code, s.site_code)
       and equal_null(lc.recipient_code, s.later_recipient_code)
       and equal_null(lc.dv_financial_year, s.dv_financial_year)
       and equal_null(lc.dv_financial_month, s.dv_financial_month)
       and equal_null(lc.commissioner_code, oc.commissioner_code)
    group by all
),

recodes as (
    select feed, dv_provider_code, site_code, dv_financial_year, dv_financial_month,
           older_recipient_code, later_recipient_code
    from recode_candidates
    where shared_commissioner_share >= 0.8
      and older_month_total <> 0
      and abs(later_month_total - older_month_total) <= 0.1 * abs(older_month_total)

    union

    select s.feed, s.dv_provider_code, s.site_code, s.dv_financial_year, s.dv_financial_month,
           s.older_recipient_code, s.later_recipient_code
    from component_successors as s
    inner join recipient_lineage as l
        on l.site_code = s.site_code
       and l.from_recipient_code = s.older_recipient_code
       and l.to_recipient_code = s.later_recipient_code
       and l.dv_financial_year = s.dv_financial_year
),

-- Retire a component only through a successor that is not itself retired.
retired_components as (
    select distinct r.feed, r.dv_provider_code, r.site_code, r.dv_financial_year, r.dv_financial_month,
           r.older_recipient_code as recipient_code
    from recodes as r
    left join recodes as onward
        on onward.feed = r.feed
       and equal_null(onward.dv_provider_code, r.dv_provider_code)
       and equal_null(onward.site_code, r.site_code)
       and equal_null(onward.dv_financial_year, r.dv_financial_year)
       and equal_null(onward.dv_financial_month, r.dv_financial_month)
       and equal_null(onward.older_recipient_code, r.later_recipient_code)
    where onward.feed is null
),

selected as (
    select e.*
    from event_files as e
    left join retired_components as r
        on {{ same_component('r', 'e') }}
    where r.feed is null
),

-- Diagnostics: commissioners carried by any earlier upload of the component
-- that the selected upload no longer carries (treated as withdrawn), valued
-- at their latest earlier statement.
omitted_commissioners as (
    select
        f.feed, f.dv_provider_code, f.site_code, f.recipient_code, f.dv_financial_year, f.dv_financial_month,
        count(*)                                as omitted_commissioner_count,
        sum(iff(f.feed = 'LSACM', c.month_total, null))
                                                as omitted_commissioner_actual_price
    from (
        select
            f.feed, f.dv_provider_code, f.site_code, f.recipient_code, f.dv_financial_year,
            f.dv_financial_month, f.meta_file_id, f.meta_batch_id, f.is_in_file_financial_year,
            f.submission_loaded_at, c.commissioner_code, sum(c.month_total) as month_total
        from ranked_file_months as f
        inner join content_groups as c
            on {{ same_component('c', 'f') }}
           and c.meta_file_id = f.meta_file_id
           and equal_null(c.meta_batch_id, f.meta_batch_id)
        left join event_files as e
            on {{ same_component('e', 'f') }}
           and e.meta_file_id = f.meta_file_id
           and equal_null(e.meta_batch_id, f.meta_batch_id)
        where e.feed is null
        group by all
        qualify row_number() over (
            partition by f.feed, f.dv_provider_code, f.site_code, f.recipient_code, f.dv_financial_year,
                         f.dv_financial_month, c.commissioner_code
            order by f.is_in_file_financial_year desc, f.submission_loaded_at desc nulls last,
                     f.meta_file_id desc, f.meta_batch_id desc
        ) = 1
    ) as c
    inner join (select distinct {{ component }} from selected) as f
        on {{ same_component('f', 'c') }}
    left join event_commissioners as ec
        on {{ same_component('ec', 'c') }}
       and equal_null(ec.commissioner_code, c.commissioner_code)
    where ec.feed is null
    group by all
),

retired_summary as (
    select feed, dv_provider_code, site_code, dv_financial_year, dv_financial_month,
           listagg(recipient_code, ',') within group (order by recipient_code) as retired_recipient_codes
    from retired_components
    group by all
)

select
    s.feed,
    s.dv_provider_code,
    s.site_code                                 as submission_site_code,
    s.recipient_code                            as submission_recipient_code,
    s.dv_financial_year,
    s.dv_financial_month,
    s.meta_file_id,
    s.meta_batch_id,
    s.submission_loaded_at,
    s.submission_file_name,
    s.is_in_file_financial_year,
    count(*) over (
        partition by s.feed, s.dv_provider_code, s.site_code, s.dv_financial_year, s.dv_financial_month
    )                                           as files_in_slice,
    -- Component-level diagnostics sit on the component's latest file only, so
    -- they can be summed.
    iff(s.component_rank = 1, coalesce(o.omitted_commissioner_count, 0), 0)
                                                as omitted_commissioner_count,
    iff(s.component_rank = 1, o.omitted_commissioner_actual_price, null)
                                                as omitted_commissioner_actual_price,
    rs.retired_recipient_codes
from selected as s
left join omitted_commissioners as o
    on {{ same_component('o', 's') }}
left join retired_summary as rs
    on {{ same_slice('rs', 's') }}
