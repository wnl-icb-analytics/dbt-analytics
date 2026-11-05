{{
    config(
        materialized='table',
        tags=['fa_checks'])
}}
/* Streamlit dsahboard base to track monthly submissions required for contract monitoring LSACM, LSPLCM, LSDrPLCM, LSDePLCM, LSREF. Returns the latest submitted file for each provider and dataset, while retaining expected provider/dataset combinations for which no submission has been received.
 */
--Shortnames for reporting
WITH expected_providers AS (
SELECT DISTINCT
        provider_code,
        provider_shorthand,
        provider_name
FROM {{ ref('stg_reference_wnl_reporting_names') }}
--FROM STAGING.REFERENCE.STG_REFERENCE_WNL_REPORTING_NAMES
where provider_code in ('RAL','RKE','RRV','RAN','RP4','RP6','R1K','RYJ','RQM','RAS')
)
--expected local standard data feeds
,expected_datasets AS (
SELECT column1::VARCHAR AS dataset
    FROM VALUES
        ('LSDePLCM'),
        ('LSDrPLCM'),
        ('LSPLCM'),
        ('LSACM'),
        ('LSREF')
)
-- Create the complete provider x dataset matrix
,expected_provider_datasets AS (
SELECT
    p.provider_code,
    p.provider_shorthand,
    p.provider_name,
    d.dataset
FROM expected_providers AS p
CROSS JOIN expected_datasets AS d
)
--five monthly dataset submissions for 10 providers
,monthly_submissions AS (
SELECT 
r.feed as dataset,
r.dv_provider_code as provider_code, 
file_id as meta_file_id, 
r.financial_month as fy_month_submitted,
r.financial_year,
r.file_version,
s.submission_date as due_date,
DATE(r.created_datetime) AS date_received,
DATE(r.sent_datetime) AS date_submitted,
r.row_count as submitted_row_count
FROM {{ ref('stg_sdl_meta_file_registry') }} r
--FROM STAGING.SDL.STG_SDL_META_FILE_REGISTRY r
LEFT JOIN {{ ref('fa_lsacm_submission_dates') }} s on s.submission_year = r.financial_year and s.fy_submission_month_number = r.financial_month
--LEFT JOIN MODELLING.CONTRACTING.FA_LSACM_SUBMISSION_DATES s on s.submission_year = r.financial_year and s.fy_submission_month_number = r.financial_month
WHERE r.feed in ('LSDePLCM','LSDrPLCM','LSPLCM','LSACM','LSREF')
AND r.dv_provider_code in ('RAL','RKE','RRV','RAN','RP4','RP6','R1K','RYJ','RQM','RAS')
AND r.financial_year = (select max(financial_year) from {{ ref('stg_sdl_meta_file_registry') }} where feed in ('LSDePLCM','LSDrPLCM','LSPLCM','LSACM','LSREF'))
--AND r.financial_year = (select max(financial_year) from STAGING.SDL.STG_SDL_META_FILE_REGISTRY where feed in ('LSDePLCM','LSDrPLCM','LSPLCM','LSACM','LSREF'))
--ensure that the latest version of the file is selected
QUALIFY ROW_NUMBER() OVER (PARTITION BY feed, r.dv_provider_code,  r.financial_year, r.financial_month  ORDER BY r.sent_datetime DESC, r.file_id DESC ) =1
)
-- From the monthly files, find the latest submitted month separately for each provider and dataset
,latest_provider_dataset_submission AS (
SELECT
    dataset,
    provider_code,
    financial_year,
    fy_month_submitted,
    meta_file_id,
    file_version,
    due_date,
    date_submitted,
    submitted_row_count
FROM monthly_submissions
QUALIFY ROW_NUMBER() OVER (PARTITION BY provider_code, dataset ORDER BY
            financial_year DESC, fy_month_submitted DESC, date_submitted DESC, meta_file_id DESC ) = 1
)
-- Find the latest month received anywhere across all providers/datasets
,latest_month_overall AS (
SELECT
    MAX(fy_month_submitted) AS max_fy_month_submitted
FROM monthly_submissions
)
SELECT
    e.provider_code,
    e.provider_shorthand,
    e.provider_name,
    e.dataset,
    s.financial_year,
    s.fy_month_submitted,
    s.meta_file_id,
    s.file_version,
    s.due_date,
    s.date_submitted,
    s.submitted_row_count,
     CASE
    WHEN e.provider_shorthand = 'MEH' AND e.dataset = 'LSDePLCM' THEN 'Not applicable'
    WHEN e.provider_shorthand = 'LNWH' AND e.dataset = 'LSREF' THEN 'Not applicable'
    WHEN s.fy_month_submitted = l.max_fy_month_submitted THEN 'Yes'
        ELSE 'No'
    END AS latest_month_received,
    --MAYBE SOME AGREED LEEWAY HERE FOR PROVIDERS BEING LATE
    CASE
    WHEN e.provider_shorthand = 'MEH' AND e.dataset = 'LSDePLCM' THEN 'Exempt'
    WHEN e.provider_shorthand = 'LNWH' AND e.dataset = 'LSREF' THEN 'Not onboarded'
    --WHEN s.fy_month_submitted < l.max_fy_month_submitted THEN 'Missed'
    WHEN s.date_submitted > due_date then 'Submitted late'  
    WHEN s.date_submitted <= due_date then 'Submitted on time' 
    ELSE 'Not Submitted' END AS submission_status
FROM expected_provider_datasets e
LEFT JOIN latest_provider_dataset_submission s ON e.provider_code = s.provider_code AND e.dataset = s.dataset
CROSS JOIN latest_month_overall AS l
ORDER BY e.dataset,e.provider_code

