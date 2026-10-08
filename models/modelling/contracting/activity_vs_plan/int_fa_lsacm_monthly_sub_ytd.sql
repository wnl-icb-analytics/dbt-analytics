{{
    config(
        materialized='table',
        tags=['fa_checks'])
}}
/* this tables pulls all records of monthly submissions into LSACM for the latest financial year and month. 
This can be used to track submissions, flag overdue and check versions for the current month and historically. No filtering by latest version or latest financial month applied.
 */
 WITH ALL_SUBMISSIONS AS (
 SELECT 
 LEFT(SPLIT_PART(r.file_name, '_', 2),3) as provider_code, 
o.organisation_name,
file_id as meta_file_id, 
TO_CHAR(DATEADD(MONTH,r.financial_month + 2,TO_DATE(r.financial_year || '-01-01')),'YYYY-MM') AS activity_month,
r.financial_month as fy_month_submitted,
s.financial_year,
SPLIT_PART(r.file_name, '_', 8) as file_version,
s.submission_date as required_submission_date,
DATE(r.created_datetime) AS date_submitted,
r.row_count as submitted_row_count
FROM {{ ref('stg_sdl_meta_file_registry') }} r
--from STAGING.SDL.STG_SDL_META_FILE_REGISTRY r
LEFT JOIN {{ ref('stg_dictionary_dbo_organisation') }} o on LEFT(SPLIT_PART(r.file_name, '_', 2),3) = o.organisation_code
--LEFT JOIN STAGING.DICTIONARY.STG_DICTIONARY_DBO_ORGANISATION o on LEFT(SPLIT_PART(r.file_name, '_', 2),3) = o.organisation_code
LEFT JOIN {{ ref('fa_lsacm_submission_dates') }} s on s.activity_year_month = TO_CHAR(DATEADD(MONTH,r.financial_month + 2,TO_DATE(r.financial_year || '-01-01')),'YYYY-MM')
--LEFT JOIN DEV__MODELLING.CONTRACTING.FA_LSACM_SUBMISSION_DATES s on s.activity_year_month = TO_CHAR(DATEADD(MONTH,r.financial_month + 2,TO_DATE(r.financial_year || '-01-01')),'YYYY-MM')
WHERE r.feed = 'LSACM'
AND LEFT(SPLIT_PART(r.file_name, '_', 2),3) in ('RAL','RKE','RRV','RAN','RP4','RP6','R1K','RYJ','RQM','RAS')
-- AND r.financial_year = (select max(financial_year) from STAGING.SDL.STG_SDL_META_FILE_REGISTRY where feed = 'LSACM')
AND r.financial_month = (select max(financial_month) from {{ ref('stg_sdl_meta_file_registry') }} where feed = 'LSACM')
--keep submission version history and all months for now. Latest version and latest financial month can be filtered for in downstream tables
--QUALIFY ROW_NUMBER() OVER (PARTITION BY LEFT(SPLIT_PART(FILE_NAME, '_', 2),3),  FINANCIAL_MONTH  ORDER BY CREATED_DATETIME  DESC) =1
)
SELECT *,
    CASE
    WHEN date_submitted is not null and date_submitted < required_submission_date then 'Submitted early'
    WHEN date_submitted is not null and date_submitted > required_submission_date+3 then 'Submitted late'      
    ELSE 'Submitted on time' END AS submission_status
from ALL_SUBMISSIONS 
order by 1, 4