{{
    config(materialized = 'table')
}}

-- latest record per practice code from the UKHFD ODS SCD table; codes are
-- upper-cased because providers submit them in mixed case downstream
--
-- Source: UKHFD.ODS.dim_All_GP_and_GDP_Practices_SCD
--   source('ukhfd_ods', 'all_gp_and_gdp_practices') -> raw_ukhfd_all_gp_and_gdp_practices
select
    upper(organisation_code) as organisation_code
    , organisation_name
from {{ ref('raw_ukhfd_all_gp_and_gdp_practices') }}
where is_latest = 1
qualify row_number() over (
    partition by upper(organisation_code)
    order by effective_from desc
) = 1
