select
    s.mhs003_uniq_id as accommodation_observation_id
    , s.mhs003_uniq_id as source_row_id
    , 'MHS003' as source_table
    , s.person_id
    , b.sk_patient_id
    , s.accommodation_status_code as legacy_accommodation_status_code
    , s.accommodation_type as accommodation_type_code
    , s.settled_accommodation_ind as settled_accommodation_code
    , s.accommodation_status_date as legacy_status_recorded_date
    , s.accommodation_type_date as type_recorded_date
    , s.accommodation_type_start_date as accommodation_start_date
    , s.accommodation_type_end_date as accommodation_end_date
    , status_label.description as legacy_accommodation_status_description
    , coalesce(type_label.description, legacy_type_label.description) as accommodation_type_description
    , case when type_label.description is not null then 'accommodation_type'
        when legacy_type_label.description is not null then 'legacy_accommodation_status'
        when nullif(trim(s.accommodation_type), '') is null then 'code_missing'
        else 'code_unmatched' end as accommodation_type_label_basis
    , settled_label.description as settled_accommodation_description
    , s.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , s.uniq_submission_id as submission_id
    , s.reporting_period_start_date
    , s.reporting_period_end_date
    , s.effective_from as source_file_received_at
from {{ ref('stg_mhsds_accommodation') }} as s
left join {{ ref('stg_mhsds_bridging') }} as b on s.person_id = b.person_id
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(s.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('mhsds_domain_code_lookup') }} as status_label
    on upper(trim(s.accommodation_status_code::varchar)) = status_label.code and status_label.code_set_name = 'accommodation_status'
left join {{ ref('mhsds_domain_code_lookup') }} as type_label
    on upper(trim(s.accommodation_type::varchar)) = type_label.code and type_label.code_set_name = 'accommodation_type'
left join {{ ref('mhsds_domain_code_lookup') }} as settled_label
    on upper(trim(s.settled_accommodation_ind::varchar)) = settled_label.code and settled_label.code_set_name = 'settled_accommodation'
left join {{ ref('mhsds_domain_code_lookup') }} as legacy_type_label
    on upper(trim(s.accommodation_type::varchar)) = legacy_type_label.code
    and legacy_type_label.code_set_name = 'accommodation_status'
