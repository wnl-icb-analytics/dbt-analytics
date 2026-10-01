select
    deficit,
    snomedct_conceptid,
    ctv3,
    provenance,
    code_description,
    time_constraint_years,
    age_limit,
    other_instructions,
    new_descriptions
from {{ ref('raw_common_efi2_codelists') }}
