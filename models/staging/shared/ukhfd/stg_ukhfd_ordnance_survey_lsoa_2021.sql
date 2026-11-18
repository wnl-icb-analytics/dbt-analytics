-- Source: UKHFD.Ordnance_Survey.dim_LSOA_DEC_2021_EW_BFE
--   source('ukhfd_ordnance_survey', 'lsoa_2021') -> raw_ukhfd_ordnance_survey_lsoa_2021
select
    lsoa21_cd as lsoa_code_2021,
    lsoa21_nm as lsoa_name_2021
from {{ ref('raw_ukhfd_ordnance_survey_lsoa_2021') }}
