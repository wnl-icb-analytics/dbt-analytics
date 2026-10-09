with listed_period as (
    select
        hrg_code,
        min(valid_from_date) as first_listed_date,
        max(coalesce(valid_to_date, '9999-12-31'::date)) as last_listed_date
    from {{ ref('hrg_code_history') }}
    where is_listed
    group by hrg_code
),

chapter as (
    select hrg_chapter_code, hrg_chapter
    from {{ ref('stg_ukhfd_pbr_hrg4_chapter') }}
    qualify row_number() over (
        partition by hrg_chapter_code order by valid_from_date desc
    ) = 1
),

subchapter as (
    select hrg_subchapter_code, hrg_subchapter
    from {{ ref('stg_ukhfd_pbr_hrg4_subchapter') }}
    qualify row_number() over (
        partition by hrg_subchapter_code order by valid_from_date desc
    ) = 1
)

select
    latest.hrg_code,
    latest.hrg_name,
    left(latest.hrg_code, 1) as hrg_chapter_code,
    chapter.hrg_chapter,
    left(latest.hrg_code, 2) as hrg_subchapter_code,
    subchapter.hrg_subchapter,
    latest.is_listed as is_in_latest_payment_list,
    listed_period.first_listed_date,
    iff(latest.is_listed, null, listed_period.last_listed_date) as last_listed_date,
    latest.source_imported_at
from {{ ref('hrg_code_history') }} as latest
left join listed_period
    on latest.hrg_code = listed_period.hrg_code
left join chapter
    on left(latest.hrg_code, 1) = chapter.hrg_chapter_code
left join subchapter
    on left(latest.hrg_code, 2) = subchapter.hrg_subchapter_code
where latest.is_latest_revision
