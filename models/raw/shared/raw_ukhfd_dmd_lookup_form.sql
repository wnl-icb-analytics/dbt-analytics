select
    "CD" as cd,
    "CDDT" as cddt,
    "CDPREV" as cdprev,
    "DESC" as desc,
    "Unique_Column" as unique_column,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_dmd', 'dmd_lookup_form') }}
