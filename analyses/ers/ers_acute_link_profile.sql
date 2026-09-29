-- Broad source and linkage-quality totals; no source identifiers are returned.
select acute_source, patient_key_agreement, count(*) as link_count
from {{ ref('rel_ers_referral_acute_record') }}
group by acute_source, patient_key_agreement
