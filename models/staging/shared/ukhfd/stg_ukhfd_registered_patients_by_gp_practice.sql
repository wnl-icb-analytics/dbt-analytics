-- UKHFD registered patient counts. One row per organisation, sex and 5-year
-- age band per monthly snapshot. Includes GP practice, PCN, ICB and other
-- organisation types; sex and age band include ALL rollup rows.
--
-- Source: UKHFD.Demography.fact_No_Of_Patients_Regd_At_GP_Prac_Regions_5Yr_AgeBand
--   source('ukhfd_demography', 'registered_patients_by_gp_practice')
--   -> raw_ukhfd_registered_patients_by_gp_practice
select
    org_type,
    org_code,
    ons_code,
    sex,
    age_band,
    number_of_patients::int as number_of_patients,
    effective_snapshot_date as snapshot_date,
    data_source_file_for_this_snapshot_version as source_file_version
from {{ ref('raw_ukhfd_registered_patients_by_gp_practice') }}
where org_code is not null
  and number_of_patients is not null
