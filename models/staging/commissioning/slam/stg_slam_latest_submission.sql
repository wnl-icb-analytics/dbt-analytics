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
--     weight under the same commissioner codes and their weights are within
--     10%, or the recode is listed in recipient_lineage. Weight is absolute
--     actual price for LSACM and row count for the patient-level feeds.

{% set feeds = [
    ('LSACM', 'stg_lsacm'),
    ('LSPLCM', 'stg_lsplcm'),
    ('LSDrPLCM', 'stg_lsdrplcm'),
    ('LSDePLCM', 'stg_lsdeplcm'),
] %}
{% set component = 'feed, dv_provider_code, site_code, recipient_code, dv_financial_year, dv_financial_month' %}
{% set file_order = 'is_in_file_financial_year desc, submission_loaded_at desc nulls last, meta_file_id desc, meta_batch_id desc' %}

with recipient_lineage as (
    -- Reviewed recipient recodes the weight test cannot detect because the
    -- commissioner codes changed at the same time.
    select *
    from (values
        -- RWH 2024/25: Flex to 93C restated by Freeze to 06K (commissioner 14E -> 13R)
        ('RWH00', '93C', '06K', '202425')
    ) as v (site_code, from_recipient_code, to_recipient_code, dv_financial_year)
),

commissioner_months as (
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
        submission_loaded_at,
        submission_file_name,
        count(*)                                as row_count,
        {% if feed == 'LSACM' %}
        coalesce(sum(abs(dv_actual_price)), 0)  as weight,
        coalesce(sum(dv_actual_price), 0)       as actual_price
        {% else %}
        count(*)                                as weight,
        null::number(38, 2)                     as actual_price
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
    from commissioner_months
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
    from commissioner_months as c
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

-- Commissioner weight per component-month inside the latest upload, counting a
-- commissioner repeated across copies once (latest file wins).
event_commissioners as (
    select
        e.feed, e.dv_provider_code, e.site_code, e.recipient_code,
        e.dv_financial_year, e.dv_financial_month, c.commissioner_code, c.weight
    from event_files as e
    inner join commissioner_months as c
        on c.feed = e.feed
       and c.meta_file_id = e.meta_file_id
       and equal_null(c.meta_batch_id, e.meta_batch_id)
       and equal_null(c.dv_provider_code, e.dv_provider_code)
       and equal_null(c.site_code, e.site_code)
       and equal_null(c.dv_financial_year, e.dv_financial_year)
       and equal_null(c.dv_financial_month, e.dv_financial_month)
    qualify row_number() over (
        partition by e.feed, e.dv_provider_code, e.site_code, e.recipient_code,
                     e.dv_financial_year, e.dv_financial_month, c.commissioner_code
        order by e.submission_loaded_at desc nulls last, e.meta_file_id desc, e.meta_batch_id desc
    ) = 1
),

components as (
    select
        e.feed, e.dv_provider_code, e.site_code, e.recipient_code, e.dv_financial_year, e.dv_financial_month,
        boolor_agg(e.component_latest_in_year)  as is_in_file_financial_year,
        max(e.component_latest_loaded_at)       as latest_loaded_at,
        max(w.weight)                           as weight
    from event_files as e
    left join (
        select {{ component }}, sum(weight) as weight
        from event_commissioners
        group by all
    ) as w
        on w.feed = e.feed
       and equal_null(w.dv_provider_code, e.dv_provider_code)
       and equal_null(w.site_code, e.site_code)
       and equal_null(w.recipient_code, e.recipient_code)
       and equal_null(w.dv_financial_year, e.dv_financial_year)
       and equal_null(w.dv_financial_month, e.dv_financial_month)
    group by all
),

-- Share of the older component's weight whose commissioner code also appears
-- in the later component, for each later component at the same site and month.
recode_candidates as (
    select
        older.feed, older.dv_provider_code, older.site_code, older.dv_financial_year, older.dv_financial_month,
        older.recipient_code                    as older_recipient_code,
        later.recipient_code                    as later_recipient_code,
        older.weight                            as older_weight,
        later.weight                            as later_weight,
        sum(iff(lc.commissioner_code is not null, oc.weight, 0)) / nullif(older.weight, 0)
                                                as shared_commissioner_share
    from components as older
    inner join components as later
        on later.feed = older.feed
       and equal_null(later.dv_provider_code, older.dv_provider_code)
       and equal_null(later.site_code, older.site_code)
       and equal_null(later.dv_financial_year, older.dv_financial_year)
       and equal_null(later.dv_financial_month, older.dv_financial_month)
       and not equal_null(later.recipient_code, older.recipient_code)
       and (
            later.is_in_file_financial_year and not older.is_in_file_financial_year
            or (later.is_in_file_financial_year = older.is_in_file_financial_year
                and later.latest_loaded_at > older.latest_loaded_at)
       )
    inner join event_commissioners as oc
        on oc.feed = older.feed
       and equal_null(oc.dv_provider_code, older.dv_provider_code)
       and equal_null(oc.site_code, older.site_code)
       and equal_null(oc.recipient_code, older.recipient_code)
       and equal_null(oc.dv_financial_year, older.dv_financial_year)
       and equal_null(oc.dv_financial_month, older.dv_financial_month)
    left join event_commissioners as lc
        on lc.feed = later.feed
       and equal_null(lc.dv_provider_code, later.dv_provider_code)
       and equal_null(lc.site_code, later.site_code)
       and equal_null(lc.recipient_code, later.recipient_code)
       and equal_null(lc.dv_financial_year, later.dv_financial_year)
       and equal_null(lc.dv_financial_month, later.dv_financial_month)
       and equal_null(lc.commissioner_code, oc.commissioner_code)
    group by all
),

recodes as (
    select feed, dv_provider_code, site_code, dv_financial_year, dv_financial_month,
           older_recipient_code, later_recipient_code
    from recode_candidates
    where shared_commissioner_share >= 0.8
      and older_weight > 0
      and abs(later_weight - older_weight) <= 0.1 * older_weight

    union

    select c.feed, c.dv_provider_code, c.site_code, c.dv_financial_year, c.dv_financial_month,
           c.recipient_code, l.to_recipient_code
    from components as c
    inner join recipient_lineage as l
        on l.site_code = c.site_code
       and l.from_recipient_code = c.recipient_code
       and l.dv_financial_year = c.dv_financial_year
    inner join components as successor
        on successor.feed = c.feed
       and equal_null(successor.dv_provider_code, c.dv_provider_code)
       and successor.site_code = c.site_code
       and successor.recipient_code = l.to_recipient_code
       and equal_null(successor.dv_financial_year, c.dv_financial_year)
       and equal_null(successor.dv_financial_month, c.dv_financial_month)
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
        on r.feed = e.feed
       and equal_null(r.dv_provider_code, e.dv_provider_code)
       and equal_null(r.site_code, e.site_code)
       and equal_null(r.recipient_code, e.recipient_code)
       and equal_null(r.dv_financial_year, e.dv_financial_year)
       and equal_null(r.dv_financial_month, e.dv_financial_month)
    where r.feed is null
),

-- Diagnostics: commissioners in the component's previous upload that the
-- selected upload no longer carries (treated as withdrawn).
previous_files as (
    select f.*
    from ranked_file_months as f
    where f.component_rank > 1
      and not (
            f.is_in_file_financial_year = f.component_latest_in_year
            and f.submission_loaded_at >= dateadd(hour, -12, f.component_latest_loaded_at)
      )
    qualify row_number() over (partition by {{ component }} order by {{ file_order }}) = 1
),

omitted_commissioners as (
    select
        p.feed, p.dv_provider_code, p.site_code, p.recipient_code, p.dv_financial_year, p.dv_financial_month,
        count(*)                                as omitted_commissioner_count,
        sum(pc.actual_price)                    as omitted_commissioner_actual_price
    from previous_files as p
    inner join commissioner_months as pc
        on pc.feed = p.feed
       and pc.meta_file_id = p.meta_file_id
       and equal_null(pc.meta_batch_id, p.meta_batch_id)
       and equal_null(pc.dv_provider_code, p.dv_provider_code)
       and equal_null(pc.site_code, p.site_code)
       and equal_null(pc.dv_financial_year, p.dv_financial_year)
       and equal_null(pc.dv_financial_month, p.dv_financial_month)
    left join event_commissioners as ec
        on ec.feed = p.feed
       and equal_null(ec.dv_provider_code, p.dv_provider_code)
       and equal_null(ec.site_code, p.site_code)
       and equal_null(ec.recipient_code, p.recipient_code)
       and equal_null(ec.dv_financial_year, p.dv_financial_year)
       and equal_null(ec.dv_financial_month, p.dv_financial_month)
       and equal_null(ec.commissioner_code, pc.commissioner_code)
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
    coalesce(o.omitted_commissioner_count, 0)   as omitted_commissioner_count,
    o.omitted_commissioner_actual_price,
    rs.retired_recipient_codes
from selected as s
left join omitted_commissioners as o
    on o.feed = s.feed
   and equal_null(o.dv_provider_code, s.dv_provider_code)
   and equal_null(o.site_code, s.site_code)
   and equal_null(o.recipient_code, s.recipient_code)
   and equal_null(o.dv_financial_year, s.dv_financial_year)
   and equal_null(o.dv_financial_month, s.dv_financial_month)
left join retired_summary as rs
    on rs.feed = s.feed
   and equal_null(rs.dv_provider_code, s.dv_provider_code)
   and equal_null(rs.site_code, s.site_code)
   and equal_null(rs.dv_financial_year, s.dv_financial_year)
   and equal_null(rs.dv_financial_month, s.dv_financial_month)
