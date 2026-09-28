select
    s.mhs011_uniq_id as social_circumstance_observation_id
    , s.mhs011_uniq_id as source_row_id
    , 'MHS011' as source_table
    , s.person_id
    , b.sk_patient_id
    , s.soc_per_circumstance as circumstance_code
    , s.soc_per_circumstance_rec_timestamp as circumstance_recorded_at
    , s.soc_per_circumstance_rec_date as circumstance_recorded_date
    , circumstance_label.preferred_term as circumstance_description
    , s.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , s.uniq_submission_id as submission_id
    , s.reporting_period_start_date
    , s.reporting_period_end_date
    , s.effective_from as source_file_received_at
from {{ ref('stg_mhsds_social_circumstance') }} as s
left join {{ ref('stg_mhsds_bridging') }} as b on s.person_id = b.person_id
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(s.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('snomed_concept') }} as circumstance_label on trim(s.soc_per_circumstance::varchar) = circumstance_label.snomed_code
