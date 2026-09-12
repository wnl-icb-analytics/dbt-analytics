SELECT
    id,
    effective_time,
    active,
    module_id,
    ref_set_id,
    referenced_component_id
FROM {{ ref('raw_nhsd_snomed_sct_refset_simple') }}
