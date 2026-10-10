{{
    config(
        materialized='table',
        tags=['fa_checks'])
}}
--testing monitoring table
with expected_providers as (
/*    Using current provider list rather than historical is safer because providers may enter or leave the submission programme. */
    SELECT distinct
        provider_code,
        organisation_name
        FROM {{ ref('int_fa_lsacm_monthly_sub_ytd_latest') }}
    --FROM DEV__MODELLING.CONTRACTING.INT_FA_LSACM_MONTHLY_SUB_YTD_LATEST
),
submission_dates as (
    SELECT
        financial_year,
        activity_year_month,
        fy_submission_month_number,
        submission_month,
        submission_date as required_submission_date,
        lead(required_submission_date) over (
            order by required_submission_date
        ) as next_required_submission_date
    FROM {{ ref('fa_lsacm_submission_dates') }}
    -- FROM DEV__MODELLING.CONTRACTING.FA_LSACM_SUBMISSION_DATES
    order by activity_month
),
provider_submissions as (
    SELECT 
    provider_code,
    organisation_name,
	meta_file_id,
	activity_month,
    fy_month_submitted,
    financial_year,
    required_submission_date,
 	date_submitted,
	file_version,
	submitted_row_count,
	submission_status
    FROM {{ ref('int_fa_lsacm_monthly_sub_ytd') }}
    -- FROM DEV__MODELLING.CONTRACTING.INT_FA_LSACM_MONTHLY_SUB_YTD
 /*   DE-DUPLICATE against multiple submissions for the same provider/month. Keep the latest submission, with the highest file ID as a final tie-breaker.   */
    QUALIFY row_number() over (partition by provider_code, financial_year, fy_month_submitted order by DATE_SUBMITTED desc, meta_file_id desc) = 1
),
monitor as (
    SELECT
        p.provider_code,
        p.organisation_name,
        s.meta_file_id,
        s.activity_month,
        d.fy_submission_month_number,
        s.fy_month_submitted,
        d.financial_year,
        d.required_submission_date,
        s.date_submitted,
        s.file_version,
        s.submitted_row_count,
        CASE
           WHEN s.date_submitted is null and current_date() > d.required_submission_date 
           and current_date() < coalesce(d.next_required_submission_date,'9999-12-31'::date) THEN 'Overdue' 
           WHEN s.date_submitted is null and current_date() >= d.next_required_submission_date THEN 'Missing' 
           WHEN s.date_submitted is null and current_date() <= d.required_submission_date THEN 'Awaiting submission'
        ELSE s.submission_status end as DASHBOARD_STATUS,
        case
            when s.DATE_SUBMITTED is not null then
                datediff('day',d.required_submission_date,s.DATE_SUBMITTED)
            when current_date() > d.required_submission_date then
                datediff('day', d.required_submission_date, current_date() )
        end as days_late,
        datediff('day', current_date(), d.required_submission_date) as days_to_submission
    FROM expected_providers p
    cross join submission_dates d
    left join provider_submissions s
        on  p.provider_code = s.provider_code
        and d.financial_year = s.financial_year
        and d.activity_year_month = s.activity_month
)
SELECT *
from monitor
order by  required_submission_date, provider_code


