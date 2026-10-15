select
    nullif(trim(presentation_pack_level), '') as presentation_pack_level,
    nullif(trim(vmp_vmpp_amp_ampp), '') as vmp_vmpp_amp_ampp,
    nullif(trim(bnf_code), '') as bnf_code,
    nullif(trim(bnf_name), '') as bnf_name,
    nullif(trim(snomed_code), '') as snomed_code,
    nullif(trim(dm_d_product_description), '') as dm_d_product_description,
    nullif(trim(strength), '') as strength,
    nullif(trim(unit_of_measure), '') as unit_of_measure,
    nullif(trim(dm_d_product_pack_description), '') as dm_d_product_pack_description,
    nullif(trim(pack), '') as pack,
    nullif(trim(sub_pack), '') as sub_pack,
    nullif(trim(vtm), '') as vtm,
    nullif(trim(vtm_name), '') as vtm_name
from {{ ref('raw_reference_bnf_latest') }} -- Source frequently carries whitespace/empty strings instead of NULLs (mostly VTM, BNF code, unit, pack desc, pack and subpack but applying to all in case)
