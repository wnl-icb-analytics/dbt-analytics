{{
    config(
        materialized = 'table',
        tags = ['sdl', 'slam']
    )
}}

-- Latest-submission lookup for the 4 SLAM feeds: one row per
-- (feed, provider, FY, month) slice identifying the file that holds the
-- current provider statement of that slice. Rebuilt fully each run (slice
-- grain is tiny), so it never goes stale while the stg_ls* tables stay pure
-- append.
--
-- Why slice grain, and why registry load time:
--   * SLAM files are cumulative-YTD restatements (median 4-6 months/file);
--     summing without latest-only filtering double-counts ~8x on a full FY.
--   * meta_file_id is NOT monotonic with load time (~45% inversions), and the
--     provider-typed DATE_AND_TIME_DATA_SET_CREATED is unreliable (~10% of
--     files contain >1 value) — so ordering uses
--     META_FILE_REGISTRY.CREATED_DATETIME (ISL processing log).
--   * A submission split across files (e.g. M1_6 + M7_12, same batch)
--     resolves correctly because the winner is picked per month-slice.
--
-- Consumers: the stg_*_latest views join this back to the staging tables;
-- or join it yourself on (feed, dv_provider_code, dv_financial_year,
-- dv_financial_month, meta_file_id, meta_batch_id).

{% set feeds = [
    ('LSACM', 'LSACM'),
    ('LSPLCM', 'LSPLCM'),
    ('LSDrPLCM', 'LSDRPLCM'),
    ('LSDePLCM', 'LSDEPLCM'),
] %}

with registry as (
    select
        feed,
        file_id,
        batch_id,
        created_datetime                        as submission_loaded_at,
        original_file_name                      as submission_file_name,
        -- FY token from the platform-generated file-name prefix,
        -- e.g. 'PLCM_2627_InformationStandard...' -> '202627'
        case
            when try_to_number(substr(regexp_substr(original_file_name, '_(2[0-9][0-9]{2})_InformationStandard', 1, 1, 'e', 1), 3, 2))
                 = mod(try_to_number(substr(regexp_substr(original_file_name, '_(2[0-9][0-9]{2})_InformationStandard', 1, 1, 'e', 1), 1, 2)) + 1, 100)
                then '20' || regexp_substr(original_file_name, '_(2[0-9][0-9]{2})_InformationStandard', 1, 1, 'e', 1)
        end                                     as financial_year_from_file_name
    from {{ source('sdl_wnl', 'META_FILE_REGISTRY') }}
    where feed in ('LSACM', 'LSPLCM', 'LSDrPLCM', 'LSDePLCM')
),

-- For rows whose stated period is incomplete, the slice key includes the
-- activity-date-derived period (same slam_period_date chain as the staging
-- models), so backfilled rows resolve to real month slices instead of one
-- NULL slice per provider. Must stay identical to the staging models' logic
-- or backfilled rows drop out of the *_latest views.
slices as (
    {% for feed, table_name in feeds %}
    select
        feed,
        meta_file_id,
        meta_batch_id,
        meta_provider_code,
        financial_year,
        financial_month,
        provider_code_raw,
        dv_commissioner,
        {{ slam_fy_from_date('period_date') }}      as fy_from_date,
        iff(period_date is not null,
            {{ slam_fm_from_date('period_date') }}, null)
                                                    as fm_from_date
    from (
        select
            '{{ feed }}'                                as feed,
            s.meta_file_id,
            s.meta_batch_id,
            s.meta_provider_code,
            cast(s.financial_year as varchar)           as financial_year,
            cast(s.financial_month as varchar)          as financial_month,
            s.organisation_identifier_code_of_provider  as provider_code_raw,
            {% if table_name == 'LSACM' %}
            -- LSACM DLP/Local layouts submit per-commissioner files, so the
            -- current statement is per (provider, commissioner, FY, month).
            -- Other feeds submit one comprehensive file per provider (grain
            -- stays provider-only): dv_commissioner is NULL and drops out.
            s.organisation_identifier_code_of_commissioner as dv_commissioner,
            {% else %}
            cast(null as varchar)                       as dv_commissioner,
            {% endif %}
            {% if table_name != 'LSACM' %}
            -- evaluated once per row; gated to incomplete-period rows
            case when {{ parse_slam_financial_year('s.financial_year') }} is null
                   or {{ parse_slam_financial_month('s.financial_month') }} is null
                 then {{ slam_period_date(table_name) }}
            end                                         as period_date
            {% else %}
            null::date                                  as period_date
            {% endif %}
        from {{ source('sdl_wnl', table_name) }} as s
    )
    group by all
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

enriched as (
    select
        s.feed,
        s.meta_file_id,
        s.meta_batch_id,
        s.dv_commissioner,
        coalesce(
            -- LSACM DLP/Local layouts carry the FY as a bare start year; other
            -- feeds keep the strict parse so their activity-date fallback wins.
            case when s.feed = 'LSACM'
                then {{ parse_slam_financial_year('s.financial_year', allow_bare_year=true) }}
                else {{ parse_slam_financial_year('s.financial_year') }}
            end,
            r.financial_year_from_file_name,
            s.fy_from_date
        )                                       as dv_financial_year,
        coalesce(
            {{ parse_slam_financial_month('s.financial_month') }},
            s.fm_from_date
        )                                       as dv_financial_month,
        {{ clean_organisation_id('upper(trim(coalesce(s.provider_code_raw, s.meta_provider_code)))') }}
                                                as dv_provider_code,
        r.submission_loaded_at,
        r.submission_file_name
    from slices as s
    left join registry as r
        on r.feed = s.feed
       and r.file_id = s.meta_file_id
       and r.batch_id = s.meta_batch_id
),

ranked as (
    select
        *,
        rank() over (
            partition by feed, dv_provider_code, dv_commissioner, dv_financial_year, dv_financial_month
            order by submission_loaded_at desc nulls last, meta_file_id desc, meta_batch_id desc
        )                                       as submission_rank
    from enriched
)

select distinct
    feed,
    dv_provider_code,
    dv_commissioner,
    dv_financial_year,
    dv_financial_month,
    meta_file_id,
    meta_batch_id,
    submission_loaded_at,
    submission_file_name
from ranked
where submission_rank = 1
