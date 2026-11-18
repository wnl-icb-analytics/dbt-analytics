-- One row per map revision: map IDs recur across effective dates.
select
    map_id,
    trim(ctv3_concept_id) as ctv3_code,
    trim(ctv3_term_id) as ctv3_term_id,
    ctv3_term_type,
    trim(sct_concept_id::varchar) as snomed_concept_id,
    trim(sct_description_id::varchar) as snomed_description_id,
    is_assured = '1' as is_assured,
    effective_date::date as map_effective_date,
    map_status = '1' as is_current_map,
    effective_from as source_effective_from_at,
    import_date as source_imported_at
from {{ ref('raw_ukhfd_ctv3_to_snomed_map') }}
where is_latest = 1
