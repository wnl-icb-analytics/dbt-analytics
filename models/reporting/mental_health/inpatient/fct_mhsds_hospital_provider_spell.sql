with spell_versions as (
    select
        s.*
        , try_to_decimal(s.dat_set_ver::varchar, 4, 1)
            as mhsds_specification_version
    from {{ ref('stg_mhsds_spell') }} as s
)

, spells as (
    select
        s.*
        , case
            when s.mhsds_specification_version >= 5
                then s.source_adm_mh_hosp_prov_spell
            when s.mhsds_specification_version < 5
                then s.source_adm_code_hosp_prov_spell
        end as admission_source_code
        , case
            when s.mhsds_specification_version >= 5
                then s.meth_adm_mh_hosp_prov_spell
            when s.mhsds_specification_version < 5
                then s.adm_meth_code_hosp_prov_spell
        end as admission_method_code
        , case
            when s.mhsds_specification_version >= 5
                then s.planned_dest_disch
            when s.mhsds_specification_version < 5
                then s.planned_disch_dest_code
        end as planned_discharge_destination_code
        , case
            when s.mhsds_specification_version >= 5
                then s.meth_of_disch_mh_hosp_prov_spell
            when s.mhsds_specification_version < 5
                then s.disch_meth_code_hosp_prov_spell
        end as discharge_method_code
        , case
            when s.mhsds_specification_version >= 5
                then s.dest_of_disch_hosp_prov_spell
            when s.mhsds_specification_version < 5
                then s.disch_dest_code_hosp_prov_spell
        end as discharge_destination_code
    from spell_versions as s
)

select
    s.uniq_hosp_prov_spell_id as source_record_id
    , 'MHSDS' as source_dataset
    , s.mhs501_uniq_id
    , s.hosp_prov_spell_id
    , s.service_request_id
    , s.uniq_serv_req_id as referral_source_record_id
    , referral.source_record_id is not null as has_matching_referral
    , referral.primary_service_or_team_id as referral_service_or_team_id
    , referral.primary_service_or_team_type_code as referral_service_or_team_type_code
    , referral.primary_service_or_team_type_description
        as referral_service_or_team_type_description
    , referral.source_service_or_team_type_name as referral_service_or_team_name
    , s.person_id
    , bridge.sk_patient_id
    , s.start_date_hosp_prov_spell as admission_date
    , s.start_time_hosp_prov_spell as admission_time
    , case
        when s.start_date_hosp_prov_spell is null then null
        when s.start_time_hosp_prov_spell is null
            then s.start_date_hosp_prov_spell::timestamp_ntz
        else timestamp_ntz_from_parts(
            s.start_date_hosp_prov_spell, s.start_time_hosp_prov_spell
        )
    end as admitted_at
    , case
        when s.start_date_hosp_prov_spell is null then null
        when s.start_time_hosp_prov_spell is null then 'date'
        else 'timestamp'
    end as admission_time_precision
    , s.decided_to_admit_date
    , s.decided_to_admit_time
    , case
        when s.decided_to_admit_date is null then null
        when s.decided_to_admit_time is null
            then s.decided_to_admit_date::timestamp_ntz
        else timestamp_ntz_from_parts(
            s.decided_to_admit_date, s.decided_to_admit_time
        )
    end as decided_to_admit_at
    , case
        when s.decided_to_admit_date is null then null
        when s.decided_to_admit_time is null then 'date'
        else 'timestamp'
    end as decided_to_admit_time_precision
    , s.admission_source_code
    , admission_source.description as admission_source_description
    , s.admission_method_code
    , admission_method.description as admission_method_description
    , s.estimated_disch_date_hosp_prov_spell as estimated_discharge_date
    , s.planned_disch_date_hosp_prov_spell as planned_discharge_date
    , s.planned_discharge_destination_code
    , planned_destination.description as planned_discharge_destination_description
    , s.disch_date_hosp_prov_spell as discharge_date
    , s.disch_time_hosp_prov_spell as discharge_time
    , case
        when s.disch_date_hosp_prov_spell is null then null
        when s.disch_time_hosp_prov_spell is null
            then s.disch_date_hosp_prov_spell::timestamp_ntz
        else timestamp_ntz_from_parts(
            s.disch_date_hosp_prov_spell, s.disch_time_hosp_prov_spell
        )
    end as discharged_at
    , case
        when s.disch_date_hosp_prov_spell is null then null
        when s.disch_time_hosp_prov_spell is null then 'date'
        else 'timestamp'
    end as discharge_time_precision
    , s.discharge_method_code
    , discharge_method.description as discharge_method_description
    , s.discharge_destination_code
    , discharge_destination.description as discharge_destination_description
    , s.disch_date_hosp_prov_spell is not null as has_recorded_discharge
    , iff(
        s.disch_date_hosp_prov_spell is null
        , 'no_recorded_discharge'
        , 'discharged'
    ) as source_spell_status
    , datediff(
        day, s.start_date_hosp_prov_spell, s.disch_date_hosp_prov_spell
    ) as recorded_length_of_stay_days
    , coalesce(
        s.disch_date_hosp_prov_spell < s.start_date_hosp_prov_spell, false
    ) as has_invalid_recorded_date_order
    , coalesce(
        timestamp_ntz_from_parts(
            s.disch_date_hosp_prov_spell, s.disch_time_hosp_prov_spell
        ) < timestamp_ntz_from_parts(
            s.start_date_hosp_prov_spell, s.start_time_hosp_prov_spell
        )
        , false
    ) as has_invalid_recorded_time_order
    , coalesce(
        s.decided_to_admit_date > s.start_date_hosp_prov_spell, false
    ) as has_decision_to_admit_after_admission
    , coalesce(
        timestamp_ntz_from_parts(
            s.decided_to_admit_date, s.decided_to_admit_time
        ) > timestamp_ntz_from_parts(
            s.start_date_hosp_prov_spell, s.start_time_hosp_prov_spell
        )
        , false
    ) as has_decision_to_admit_after_admission_time
    , coalesce(
        s.estimated_disch_date_hosp_prov_spell
            < s.start_date_hosp_prov_spell
        , false
    ) as has_estimated_discharge_before_admission
    , coalesce(
        s.planned_disch_date_hosp_prov_spell
            < s.start_date_hosp_prov_spell
        , false
    ) as has_planned_discharge_before_admission
    , s.age_hosp_start_date as age_at_admission
    , s.age_hosp_disch_date as age_at_discharge
    , s.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , s.dm_icb_commissioner as source_derived_icb_commissioner_code
    , commissioner.organisation_name as source_derived_icb_commissioner_name
    , coalesce(commissioner.is_wnl_commissioner, false) as is_wnl_commissioner
    , s.record_start_date
    , s.record_end_date
    , s.uniq_submission_id
    , s.uniq_month_id
    , s.reporting_period_start_date
    , s.reporting_period_end_date
    , s.mhsds_specification_version
    , s.dmic_dataset as source_pipeline_dataset
    , s.effective_from as source_file_received_at
    , s.dmic_date_added as source_loaded_at
from spells as s
left join {{ ref('fct_mhsds_referral') }} as referral
    on s.uniq_serv_req_id = referral.source_record_id
left join {{ ref('stg_mhsds_bridging') }} as bridge
    on s.person_id = bridge.person_id
left join {{ ref('mhsds_inpatient_code_lookup') }} as admission_source
    on upper(trim(s.admission_source_code)) = admission_source.code
    and admission_source.code_set_name = 'admission_source'
left join {{ ref('mhsds_inpatient_code_lookup') }} as admission_method
    on upper(trim(s.admission_method_code)) = admission_method.code
    and admission_method.code_set_name = 'admission_method'
left join {{ ref('mhsds_inpatient_code_lookup') }} as planned_destination
    on upper(trim(s.planned_discharge_destination_code)) = planned_destination.code
    and planned_destination.code_set_name = 'planned_discharge_destination'
left join {{ ref('mhsds_inpatient_code_lookup') }} as discharge_method
    on upper(trim(s.discharge_method_code)) = discharge_method.code
    and discharge_method.code_set_name = 'discharge_method'
left join {{ ref('mhsds_inpatient_code_lookup') }} as discharge_destination
    on upper(trim(s.discharge_destination_code)) = discharge_destination.code
    and discharge_destination.code_set_name = 'discharge_destination'
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(s.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as commissioner
    on upper(s.dm_icb_commissioner) = upper(commissioner.organisation_code)
