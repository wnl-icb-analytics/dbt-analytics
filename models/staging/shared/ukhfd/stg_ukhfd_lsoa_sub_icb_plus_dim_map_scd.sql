-- Mapping between LSOA and sub-ICB and higher NHS geography dimensions.
--
-- Source: UKHFD.LSOA_Sub_ICB_ICB_NHSER_LAD.dim_Map_SCD
--   source('ukhfd_lsoa_to_sub_icb_plus', 'dim_Map_SCD') -> raw_ukhfd_lsoa_sub_icb_plus_dim_map_scd
select
    lsoa_code,
    lsoa_code_year,
    sub_icb_code,
    sub_icb_code_year,
    sub_icb_ods_code,
    sub_icb_ods_code_year,
    dict_ods_icb.organisation_name as icb_name,
    icb_code,
    icb_code_year,
    icb_ods_code,
    icb_ods_code_year,
    nhser_code,
    nhser_code_year,
    nhser_ods_code,
    nhser_ods_code_year,
    dict_lad.name as lad_name,
    lad_code,
    lad_code_year,
    effective_snapshot_date,
    is_latest,
    effective_from,
    effective_to
from {{ ref('raw_ukhfd_lsoa_sub_icb_plus_dim_map_scd') }} raw

left join reference.organisation.organisation dict_ods_icb
on raw.icb_ods_code = dict_ods_icb.organisation_code

left join reference.data_dictionary.ons_geography dict_lad
on raw.lad_code = dict_lad.code