select
    vaccine,
    dose,
    proposedcluster,
    sourceclusterid,
    sourceclusterdescription,
    snomedconceptid,
    codedescription,
    source
from {{ ref('raw_reference_childhood_imms_codes') }}
