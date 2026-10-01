select
    vaccine,
    dose,
    proposedcluster,
    sourceclusterid,
    sourceclusterdescription,
    snomedconceptid,
    codedescription,
    source
from {{ ref('raw_reference_adult_imms_codes') }}