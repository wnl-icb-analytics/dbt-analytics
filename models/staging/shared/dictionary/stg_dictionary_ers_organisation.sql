select
    organisation_id,
    organisation_name
from {{ ref('raw_dictionary_ers_organisation') }}
