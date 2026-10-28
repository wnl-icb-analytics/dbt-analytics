{{
    config(
        materialized='table',
        tags=['fa_checks'])
}}
/* This table takes the latest file version and financial month from the lsacm monthly submissions table. 10 Providers one row per provider
This can be used to track submissions, flag overdue and check versions for the current financial month.
 */
WITH latest_month as (
SELECT provider_code, MAX(fy_month_submitted) as fy_month_submitted
FROM {{ ref('int_fa_lsacm_monthly_sub_ytd') }}
--FROM DEV__MODELLING.CONTRACTING.INT_FA_LSACM_MONTHLY_SUB_YTD 
group by provider_code
)

SELECT 
    y.provider_code,
    y.organisation_name,
	y.meta_file_id,
	y.activity_month,
    l.fy_month_submitted,
    y.financial_year,
    y.required_submission_date,
 	y.date_submitted,
	y.file_version,
	y.submitted_row_count,
	y.submission_status
FROM {{ ref('int_fa_lsacm_monthly_sub_ytd') }} y
--FROM DEV__MODELLING.CONTRACTING.INT_FA_LSACM_MONTHLY_SUB_YTD y
--select latest financial month by provider
INNER JOIN latest_month l ON y.provider_code = l.provider_code AND y.fy_month_submitted = l.fy_month_submitted
QUALIFY ROW_NUMBER() OVER (PARTITION BY y.provider_code,  y.fy_month_submitted  ORDER BY y.date_submitted  DESC) =1
ORDER BY provider_code
