{{
    config(
        materialized = 'table',
        tags = ['sdl', 'community_pld']
    )
}}

-- Latest-submission lookup for the community PLD feeds (COMOPL, REF): one row
-- per (feed, provider, FY, month) slice identifying the file that holds the
-- current provider statement. Rebuilt fully each run (slice grain is tiny).
--
-- Why this is needed: these are DLP feeds with cumulative restatement (Flex =
-- provisional, Freeze = final, re-sent for the same period). ~77% of slices
-- span multiple files (max 63 COMOPL / 564 REF) and summing the raw staging
-- tables double-counts ~4.7x on a full FY.
--
-- Built from the staging outputs (not re-derived from source) so the slice keys
-- — dv_provider_code / dv_financial_year / dv_financial_month — match the
-- stg_comopl / stg_ref models exactly. Ranking uses
-- META_FILE_REGISTRY.CREATED_DATETIME (ISL load time): meta_file_id is not
-- monotonic with load time, and the winner is picked per month-slice so a
-- submission split across files still resolves.
--
-- Consumers: the stg_comopl_latest / stg_ref_latest views; or join this back on
-- (feed, dv_provider_code, dv_financial_year, dv_financial_month, meta_file_id,
-- meta_batch_id). Rows with no resolvable period are absent (they cannot be
-- assigned to a slice, so latest-only filtering does not apply to them).

with slices as (
    select
        'COMOPL'            as feed,
        provider_code       as dv_provider_code,
        dv_financial_year,
        dv_financial_month,
        meta_file_id,
        meta_batch_id
    from {{ ref('stg_comopl') }}
    where dv_financial_year is not null and dv_financial_month is not null
    group by all
    union all
    select
        'REF',
        provider_code,
        dv_financial_year,
        dv_financial_month,
        meta_file_id,
        meta_batch_id
    from {{ ref('stg_ref') }}
    where dv_financial_year is not null and dv_financial_month is not null
    group by all
),

registry as (
    select
        feed,
        file_id,
        batch_id,
        created_datetime    as submission_loaded_at,
        original_file_name  as submission_file_name
    from {{ ref('raw_sdl_wnl_meta_file_registry') }}
    where feed in ('COMOPL', 'REF')
),

ranked as (
    select
        s.*,
        r.submission_loaded_at,
        r.submission_file_name,
        rank() over (
            partition by s.feed, s.dv_provider_code, s.dv_financial_year, s.dv_financial_month
            order by r.submission_loaded_at desc nulls last, s.meta_file_id desc, s.meta_batch_id desc
        )                   as submission_rank
    from slices as s
    left join registry as r
        on r.feed = s.feed
       and r.file_id = s.meta_file_id
       and r.batch_id = s.meta_batch_id
)

select
    feed,
    dv_provider_code,
    dv_financial_year,
    dv_financial_month,
    meta_file_id,
    meta_batch_id,
    submission_loaded_at,
    submission_file_name
from ranked
where submission_rank = 1
