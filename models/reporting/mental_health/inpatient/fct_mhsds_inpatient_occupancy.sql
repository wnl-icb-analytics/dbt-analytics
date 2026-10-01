select
    o.uniq_hosp_prov_spell_num as occupancy_interval_id
    , o.uniq_hosp_prov_spell_id as hospital_provider_spell_source_record_id
    , o.uniq_serv_req_id as referral_source_record_id
    , o.person_id
    , o.sk_patient_id
    , o.org_id_prov as provider_organisation_code
    , p.organisation_name as provider_organisation_name
    , o.start_date_hosp_prov_spell as admission_date
    , o.disch_date_hosp_prov_spell as recorded_discharge_date
    , o.end_date as occupancy_end_date
    , o.end_date_source as occupancy_end_reason
    , o.end_date_source = 'open' as is_current_inpatient
    , o.age_hosp_start_date as age_at_admission
    , o.dm_icb_commissioner as source_derived_icb_commissioner_code
    , comm.organisation_name as source_derived_icb_commissioner_name
    , o.reporting_period_end_date as last_submission_period_end_date
    , o.latest_period_end as occupancy_evidence_as_of_date
    , o.calculation_date
    , datediff(day, o.start_date_hosp_prov_spell,
        coalesce(o.end_date, o.reporting_period_end_date)) as occupancy_days_to_last_evidence
from {{ ref('int_mhsds_inpatient_occupancy') }} as o
left join {{ ref('int_mhsds_organisation') }} as p
    on upper(o.org_id_prov) = upper(p.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as comm
    on upper(o.dm_icb_commissioner) = upper(comm.organisation_code)
