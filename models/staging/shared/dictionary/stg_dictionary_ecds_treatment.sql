{{
    config(materialized = 'table')
}}

select
    ecds_unique_id,
    refset_unique_id,
    snomed_code,
    snomed_uk_preferred_term,
    snomed_fully_specified_name,
    ecds_description,
    ecds_group1,
    pb_r_category,
    cds_code_mapping_used_for_hrg_grouping,
    cds_treatment_mapping_that_is_used_for_hrg_grouping,
    valid_from,
    valid_to,
    dv_is_active
    -- Excluded (low analytical value):
    -- sort1, sort2, sort3, sort4, notes
from {{ ref('raw_dictionary_ecds_treatment') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ecds_unique_id, valid_from
    ORDER BY valid_to DESC NULLS LAST
) = 1