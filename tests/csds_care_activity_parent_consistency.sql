-- A linked activity must point to its own submitted contact occurrence.
-- Unmatched source records remain valid output with a null source-row link.
select a.source_record_id
from {{ ref('fct_csds_care_activity') }} as a
left join {{ ref('stg_csds_care_contact_history') }} as c
    on a.contact_source_row_id = c.cyp201_unique_id
    and a.submission_id = c.unique_submission_id
    and a.contact_id = c.unique_care_contact_identifier
where a.contact_source_row_id is not null and c.cyp201_unique_id is null
