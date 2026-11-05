-- UKHFD registered patient counts. One row per GP practice, sex and LSOA
-- code per monthly snapshot.
--
-- Source: UKHFD.Demography.fact_No_Of_Patients_Regd_At_GP_Practice_LSOA_2021_Level
--   source('ukhfd_demography', 'registered_patients_by_gp_practice_lsoa_2021')
--   -> raw_ukhfd_registered_patients_by_gp_practice_lsoa_2021

select
    gp_practice_code,
    lsoa_code,
    census_year,
    sex,
    size,
    effective_snapshot_date,
    data_source_file_for_this_snapshot_version
    
from {{ ref('raw_ukhfd_registered_patients_by_gp_practice_lsoa_2021') }}
