{{
    config(
        materialized = 'view',
        tags = ['mhsds']
    )
}}

with header as (
    select
        uniq_submission_id
        , dat_set_ver
    from {{ ref('raw_mhsds_mhs000header') }}
    qualify row_number() over (
        partition by uniq_submission_id
        order by effective_from desc, row_number desc, mhs000_uniq_id desc
    ) = 1
)

select
    submission.org_id_provider
    , submission.uniq_submission_id
    , submission.reporting_period_end_date::date as reporting_period_end_date
    , header.dat_set_ver
from {{ ref('raw_mhsds_activesubmission') }} as submission
left join header
    on submission.uniq_submission_id = header.uniq_submission_id
