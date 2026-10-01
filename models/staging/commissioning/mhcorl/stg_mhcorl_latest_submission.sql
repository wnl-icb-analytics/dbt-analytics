{{
    config(
        materialized = 'table',
        tags = ['mhcorl', 'sdl', 'mh']
    )
}}

-- Latest-submission lookup for MHCORL: one row per (dataset, provider,
-- commissioner, FY, month) slice identifying the file that holds the current
-- provider statement. Rebuilt fully each run (slice grain is tiny).
--
-- Why this is needed: MHCORL is a cumulative restatement feed. RAT00 resends
-- Flex then Freeze files for the same month, RKL00's liaison files are
-- year-to-date, and RNK00 / RV300 send separate files per commissioner
-- footprint (NCL / NWL, or per CCG). Summing stg_mhcorl double-counts.
--
-- Same approach as stg_community_pld_latest_submission and
-- stg_mh_slam_latest_submission: slice keys come from the staging output so
-- they match exactly; the winner per slice is the most recently loaded file
-- by META_FILE_REGISTRY.CREATED_DATETIME (meta_file_id is not monotonic with
-- load time), with file/batch tiebreaks. The commissioner is in the slice key
-- (the LSACM lesson) so a per-commissioner file cannot supersede the full
-- submission for every other commissioner in the same month; the dataset is
-- in the key because a provider's inpatient and community files for one
-- month are different submissions.
--
-- Consumers: the stg_mhcorl_latest view; or join back on (dv_dataset,
-- dv_provider_code, dv_commissioner, dv_financial_year, dv_financial_month,
-- meta_file_id, meta_batch_id). dv_commissioner is
-- coalesce(commissioner_code, dlp_commissioner_code); rows with neither form
-- their own slice. Rows with no resolvable period are absent (they cannot be
-- assigned to a slice, so latest-only filtering does not apply to them).

with slices as (
    select
        dv_dataset,
        provider_code       as dv_provider_code,
        coalesce(commissioner_code, dlp_commissioner_code)
                            as dv_commissioner,
        dv_financial_year,
        dv_financial_month,
        meta_file_id,
        meta_batch_id
    from {{ ref('stg_mhcorl') }}
    where dv_financial_year is not null and dv_financial_month is not null
    group by all
),

registry as (
    select
        file_id,
        batch_id,
        created_datetime    as submission_loaded_at,
        original_file_name  as submission_file_name
    from {{ ref('raw_sdl_wnl_meta_file_registry') }}
    where feed = 'MHCORL'
),

ranked as (
    select
        s.*,
        r.submission_loaded_at,
        r.submission_file_name,
        rank() over (
            partition by s.dv_dataset, s.dv_provider_code, s.dv_commissioner, s.dv_financial_year, s.dv_financial_month
            order by r.submission_loaded_at desc nulls last, s.meta_file_id desc, s.meta_batch_id desc
        )                   as submission_rank
    from slices as s
    left join registry as r
        on r.file_id = s.meta_file_id
       and r.batch_id = s.meta_batch_id
)

select
    dv_dataset,
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
