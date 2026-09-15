-- Staging model for patient care-home registrations.
-- Keeps source-system keys and validity dates available to downstream commissioning models.

select
    raw.sk_data_source_id,
    raw.sk_patient_id,
    raw.sk_organisation_id,
    raw.period_start,
    raw.period_end,
    raw.score,
    raw.date_detected_join,
    raw.date_detected_left
from {{ ref('raw_fact_patient_factcarehome') }} as raw
where raw.sk_patient_id is not null
  and raw.sk_organisation_id is not null
  and raw.period_start is not null
qualify row_number() over (
    partition by raw.sk_patient_id, raw.sk_organisation_id, raw.period_start
    order by
        raw.date_detected_join desc nulls last,
        raw.date_detected_left desc nulls last,
        raw.period_end desc nulls last,
        raw.score desc nulls last,
        raw.sk_data_source_id
) = 1
