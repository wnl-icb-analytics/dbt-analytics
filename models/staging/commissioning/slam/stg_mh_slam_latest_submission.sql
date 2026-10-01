{{
    config(
        materialized = 'table',
        tags = ['sdl', 'mh_slam']
    )
}}

-- Latest-submission lookup for the three MH Local SLAM datasets: one row per
-- (dataset, provider, FY, month) slice identifying the file that holds the
-- current provider statement. Rebuilt fully each run (slice grain is tiny).
--
-- Why this is needed: these are DLP feeds with cumulative Flex/Freeze
-- restatement (files span up to 12 months and the same period is re-sent),
-- so summing the staging tables double-counts. Same approach as
-- stg_community_pld_latest_submission / STG_SLAM_LATEST_SUBMISSION: slice
-- keys come from the staging outputs so they match exactly; the winner per
-- slice is the most recently loaded file by META_FILE_REGISTRY.CREATED_DATETIME
-- (meta_file_id is not monotonic with load time), with file/batch tiebreaks.
--
-- The slice includes the COMMISSIONER (the LSACM lesson): providers also send
-- narrowly-scoped resubmissions for a single other commissioner (e.g. an
-- RV300 '72Q Activity' file), and without the commissioner in the key such a
-- file would supersede the full-NWL submission for every month it states and
-- silently drop the W2U3Z rows (observed: all of CNWL FY2023/24). Per
-- commissioner, the full file keeps winning its own slices.
--
-- dataset values: MH_NONAPC / MH_APC (both from the MH feed) and
-- MH_REFERRALS (from the REF feed's MHRef profile).
--
-- Consumers: the stg_mh_*_latest views; or join back on (dataset,
-- dv_provider_code, dv_commissioner, dv_financial_year, dv_financial_month,
-- meta_file_id, meta_batch_id) - dv_commissioner is
-- coalesce(commissioner_code, dlp_commissioner_code) and NULL for the
-- pre-spec referral layouts (their own slice). Rows with no resolvable
-- period are absent (they cannot be assigned to a slice, so latest-only
-- filtering does not apply to them).

with slices as (
    {% for dataset, feed, model in [
        ('MH_NONAPC', 'MH', 'stg_mh_nonapc'),
        ('MH_APC', 'MH', 'stg_mh_apc'),
        ('MH_REFERRALS', 'REF', 'stg_mh_referrals'),
    ] %}
    select
        '{{ dataset }}'     as dataset,
        '{{ feed }}'        as feed,
        provider_code       as dv_provider_code,
        coalesce(commissioner_code, dlp_commissioner_code)
                            as dv_commissioner,
        dv_financial_year,
        dv_financial_month,
        meta_file_id,
        meta_batch_id
    from {{ ref(model) }}
    where dv_financial_year is not null and dv_financial_month is not null
    group by all
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

registry as (
    select
        feed,
        file_id,
        batch_id,
        created_datetime    as submission_loaded_at,
        original_file_name  as submission_file_name
    from {{ ref('raw_sdl_wnl_meta_file_registry') }}
    where feed in ('MH', 'REF')
),

ranked as (
    select
        s.*,
        r.submission_loaded_at,
        r.submission_file_name,
        rank() over (
            partition by s.dataset, s.dv_provider_code, s.dv_commissioner, s.dv_financial_year, s.dv_financial_month
            order by r.submission_loaded_at desc nulls last, s.meta_file_id desc, s.meta_batch_id desc
        )                   as submission_rank
    from slices as s
    left join registry as r
        on r.feed = s.feed
       and r.file_id = s.meta_file_id
       and r.batch_id = s.meta_batch_id
)

select
    dataset,
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
