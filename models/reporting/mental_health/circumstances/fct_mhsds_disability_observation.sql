select
    s.mhs007_uniq_id as disability_observation_id
    , s.mhs007_uniq_id as source_row_id
    , 'MHS007' as source_table
    , s.person_id
    , b.sk_patient_id
    , s.disab_code as disability_code
    , s.disab_impac_percep as disability_impact_code
    , disability_label.description as disability_description
    , impact_label.description as disability_impact_description
    , s.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , s.uniq_submission_id as submission_id
    , s.reporting_period_start_date
    , s.reporting_period_end_date
    , s.effective_from as source_file_received_at
from {{ ref('stg_mhsds_disability') }} as s
left join {{ ref('stg_mhsds_bridging') }} as b on s.person_id = b.person_id
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(s.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('mhsds_domain_code_lookup') }} as disability_label
    on upper(trim(s.disab_code::varchar)) = disability_label.code and disability_label.code_set_name = 'disability'
left join {{ ref('mhsds_domain_code_lookup') }} as impact_label
    on upper(trim(s.disab_impac_percep::varchar)) = impact_label.code and impact_label.code_set_name = 'disability_impact'
