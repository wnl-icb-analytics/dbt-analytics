select
    patient_address_id,
    status,
    matched,
    uprn,
    postcode_quality,
    qualifier,
    classification,
    algorithm,
    match_pattern,
    publisher_organisation_code
    -- REVIEW: PATIENT_UPRN no longer exposes record metadata or a deletion flag.
from {{ ref('raw_olids_patient_uprn') }}
