{{
    config(
        materialized='table',
        tags=['fa_checks'])
}}
/* This table takes the latest file version and financial month from the lsacm monthly submissions table. 10 Providers one row per provider
This can be used to track submissions, flag overdue and check versions for the current financial month.
 */
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
--FROM DEV__MODELLING.CONTRACTING.INT_FA_LSACM_MONTHLY_SUB_YTD
--select latest financial month
WHERE fy_month_submitted = (SELECT MAX(fy_month_submitted) FROM {{ ref('int_fa_lsacm_monthly_sub_ytd') }} )
--WHERE fy_month_submitted = (SELECT MAX(fy_month_submitted) FROM DEV__MODELLING.CONTRACTING.INT_FA_LSACM_MONTHLY_SUB_YTD )
--ensure that the latest version of the file is selected
QUALIFY ROW_NUMBER() OVER (PARTITION BY provider_code,  fy_month_submitted  ORDER BY date_submitted  DESC) =1
ORDER BY provider_code