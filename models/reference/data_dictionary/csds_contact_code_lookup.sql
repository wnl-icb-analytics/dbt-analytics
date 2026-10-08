-- csds_contact_code_lookup.sql

select *
from {{ ref('raw_reference_csds_lookup') }}