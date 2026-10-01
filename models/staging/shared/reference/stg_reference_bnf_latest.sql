select
    presentation_pack_level,
    vmp_vmpp_amp_ampp,
    bnf_code,
    bnf_name,
    snomed_code,
    dm_d_product_description,
    strength,
    unit_of_measure,
    dm_d_product_pack_description,
    pack,
    sub_pack,
    vtm,
    vtm_name
from {{ ref('raw_reference_bnf_latest') }}
