select
    m.uniq_mh_act_episode_id as mental_health_act_period_id
    , m.person_id
    , b.sk_patient_id
    , m.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , m.nhsd_legal_status as legal_status_code
    , label.legal_status_desc as legal_status_description
    , c.nhsd_legal_status is not null as is_classified_legal_status
    , coalesce(c.is_detained, false) as is_detention_period
    , m.start_date_mh_act_legal_status_class as legal_status_start_date
    , m.end_date_mh_act_legal_status_class as legal_status_end_date
    , m.expiry_date_mh_act_legal_status_class as legal_status_expiry_date
    , d.as_of_date
    , coalesce(c.is_detained, false)
        and m.end_date_mh_act_legal_status_class is null
        and (m.expiry_date_mh_act_legal_status_class is null
            or m.expiry_date_mh_act_legal_status_class >= d.as_of_date)
        and m.reporting_period_end_date >= dateadd(month, -2, d.as_of_date)
        as is_current_detention_period
    , m.reporting_period_end_date as last_submission_period_end_date
    , m.uniq_submission_id as submission_id
    , m.effective_from as source_file_received_at
from {{ ref('stg_mhsds_mhactperiod') }} as m
cross join {{ ref('int_mhsds_reporting_date') }} as d
left join {{ ref('mhsds_mh_act_legal_status_classification') }} as c
    on m.nhsd_legal_status = c.nhsd_legal_status
left join {{ ref('stg_ukhfd_mental_health_act_legal_status_classification') }} as label
    on m.nhsd_legal_status = label.legal_status_code
left join {{ ref('stg_mhsds_bridging') }} as b on m.person_id = b.person_id
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(m.org_id_prov) = upper(provider.organisation_code)
