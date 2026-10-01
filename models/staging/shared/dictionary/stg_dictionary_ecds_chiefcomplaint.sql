select
    ecds_unique_id,
    snomed_code,
    snomed_uk_preferred_term,
    snomed_fully_specified_name,
    ecds_description,
    ecds_group1
from {{ ref('raw_dictionary_ecds_chiefcomplaint') }}
--where ecds_unique_id != 'Code deprecated'