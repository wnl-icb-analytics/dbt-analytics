select
    old_concept_id,
    old_concept_status,
    new_concept_id,
    new_concept_status,
    path,
    is_ambiguous,
    iterations,
    old_concept_fully_specified_name,
    new_concept_fully_specified_name,
    new_concept_fully_specified_name_status,
    top_level_hierarchy_identical_flag,
    fully_specified_name_tagless_identical_flag,
    fully_specified_name_tag_identical_flag
from {{ ref('raw_nhsd_snomed_sct_history') }}
