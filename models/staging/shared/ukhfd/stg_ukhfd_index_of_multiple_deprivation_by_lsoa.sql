-- Source: UKHFD.Demography.fact_Index_Of_Multiple_Deprivation_By_LSOA
--   source('ukhfd_demography', 'index_of_multiple_deprivation_by_lsoa') -> raw_ukhfd_index_of_multiple_deprivation_by_lsoa
select
    year(effective_snapshot_date) as release_year,
    lsoa_code,
    -- UKHFD allocates E01027305 to Cumberland (E06000063) in the 2025 release.
    -- MHCLG's corrected files (17 November 2025) and ONS allocate it to West Northamptonshire.
    case
        when year(effective_snapshot_date) = 2025 and lsoa_code = 'E01027305' then 'E06000062'
        else coalesce(lad_code_2024, la_district_code)
    end as local_authority_district_code,
    imd_score::number(38, 3) as imd_score,
    imd_rank::integer as imd_rank,
    imd_decile::integer as imd_decile
from {{ ref('raw_ukhfd_index_of_multiple_deprivation_by_lsoa') }}
qualify data_source_file_for_this_snapshot_version
    = max(data_source_file_for_this_snapshot_version) over (partition by effective_snapshot_date)
