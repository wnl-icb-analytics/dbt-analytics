{{ config(materialized='view') }}

with latest_provider_status as (

    select *
    from {{ ref('dq_monitoring_provider_status') }}

    qualify row_number() over (
        partition by
            table_id,
            provider_code

        order by
            measurement_date desc,
            measured_at desc nulls last
    ) = 1

),

slam_submission_status as (

    select
        provider_code,
        provider_shorthand,
        provider_name,
        dataset,
        financial_year,
        fy_month_submitted,
        meta_file_id,
        file_version,
        due_date,
        date_submitted,
        submitted_row_count,
        latest_month_received,
        submission_status

    from {{ ref('int_ls_contract_dataset_submissions') }}

    where dataset in (
        'LSACM',
        'LSPLCM'
    )

),

enriched as (

    select
        p.*,

        -- Submission metadata is applicable to SLAM only
        s.provider_shorthand,
        s.financial_year as submitted_financial_year,
        s.fy_month_submitted,
        s.meta_file_id as submitted_meta_file_id,
        s.file_version as submitted_file_version,
        s.due_date as submission_due_date,
        s.date_submitted,
        s.submitted_row_count,
        s.latest_month_received,
        s.submission_status,

        -- Current submission completeness
        case
            when p.dataset_group <> 'SLAM'
                then 'NOT_APPLICABLE'

            when s.provider_code is null
                then 'RED'

            when s.submission_status = 'Not Submitted'
                then 'RED'

            when s.latest_month_received = 'No'
                then 'RED'

            when s.latest_month_received = 'Yes'
                then 'GREEN'

            else 'GREY'
        end as submission_completeness_status,

        -- Submission timeliness remains separate from current completeness
        case
            when p.dataset_group <> 'SLAM'
                then 'NOT_APPLICABLE'

            when s.provider_code is null
                then 'RED'

            when s.submission_status = 'Not Submitted'
                then 'RED'

            when s.submission_status = 'Submitted late'
                then 'AMBER'

            when s.submission_status = 'Submitted on time'
                then 'GREEN'

            else 'GREY'
        end as submission_timeliness_status,

        case
            when p.dataset_group <> 'SLAM'
                then null

            when s.provider_code is null
                then 'Expected provider submission not found'

            when s.submission_status = 'Not Submitted'
                then 'Expected provider submission has not been received'

            when s.latest_month_received = 'No'
                then 'Provider has not submitted the latest financial month'

            when s.submission_status = 'Submitted late'
                then 'Latest required month received, but submitted after the due date'

            when s.submission_status = 'Submitted on time'
                then 'Latest required month received on time'

            else 'Submission position could not be assessed'
        end as submission_status_reason

    from latest_provider_status p

    left join slam_submission_status s
        on p.provider_code = s.provider_code

       and (
            (
                p.table_id = 'SLAM_LSACM'
                and s.dataset = 'LSACM'
            )
            or
            (
                p.table_id = 'SLAM_LSPLCM'
                and s.dataset = 'LSPLCM'
            )
       )

)

select *
from enriched
