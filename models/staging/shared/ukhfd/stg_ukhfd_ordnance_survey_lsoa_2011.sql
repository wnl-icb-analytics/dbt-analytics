-- Source: UKHFD.Ordnance_Survey.dim_LSOA_2011
--   source('ukhfd_ordnance_survey', 'lsoa_2011') -> raw_ukhfd_ordnance_survey_lsoa_2011
select
    lsoa11_cd as lsoa_code_2011,
    lsoa11_nm as lsoa_name_2011
from {{ ref('raw_ukhfd_ordnance_survey_lsoa_2011') }}
