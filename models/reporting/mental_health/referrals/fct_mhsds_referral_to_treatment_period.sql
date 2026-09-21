select
    s.mhs104_uniq_id as referral_to_treatment_period_id
    , s.mhs104_uniq_id as source_row_id
    , 'MHS104' as source_table
    , s.person_id
    , b.sk_patient_id
    , s.uniq_serv_req_id as referral_source_record_id
    , s.org_id_pat_path_id_issuer as pathway_issuer_organisation_code
    , s.wait_time_measure_type as waiting_time_measurement_type_code
    , s.refer_to_treat_period_start_date as rtt_start_date
    , s.refer_to_treat_period_end_date as rtt_end_date
    , s.refer_to_treat_period_status as rtt_status_code
    , measure_label.description as waiting_time_measurement_type_description
    , status.rtt_period_status_description as rtt_status_description
    , status.rtt_period_status_category as rtt_status_category
    , iff(s.refer_to_treat_period_end_date >= s.refer_to_treat_period_start_date, datediff(day, s.refer_to_treat_period_start_date, s.refer_to_treat_period_end_date), null) as recorded_rtt_interval_days
    , s.refer_to_treat_period_start_date <= s.reporting_period_end_date and (s.refer_to_treat_period_end_date is null or s.refer_to_treat_period_end_date > s.reporting_period_end_date) as is_recorded_open_at_period_end
    , s.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , s.uniq_submission_id as submission_id
    , s.reporting_period_start_date
    , s.reporting_period_end_date
    , s.effective_from as source_file_received_at
    , issuer.organisation_name as pathway_issuer_organisation_name
from {{ ref('stg_mhsds_referral_to_treatment') }} as s
left join {{ ref('stg_mhsds_bridging') }} as b on s.person_id = b.person_id
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(s.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('mhsds_domain_code_lookup') }} as measure_label
    on upper(trim(s.wait_time_measure_type::varchar)) = measure_label.code and measure_label.code_set_name = 'waiting_time_measurement_type'
left join {{ ref('stg_dictionary_dbo_rttperiodstatus') }} as status on s.refer_to_treat_period_status = status.rtt_period_status_code
left join {{ ref('int_mhsds_organisation') }} as issuer
    on upper(s.org_id_pat_path_id_issuer) = upper(issuer.organisation_code)
