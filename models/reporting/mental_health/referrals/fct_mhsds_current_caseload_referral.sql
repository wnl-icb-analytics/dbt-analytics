{{ config(materialized='view') }}

select *
from {{ ref('fct_mhsds_latest_provider_caseload_referral') }}
where reporting_period_end_date = dataset_latest_reporting_period_end_date
