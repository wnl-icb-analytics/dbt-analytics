select
    {{ dbt_utils.generate_surrogate_key([
        'a.org_id_prov',
        'a.reporting_period_end_date',
        'a.uniq_care_act_id'
    ]) }} as source_record_id
    , 'MHSDS' as source_dataset
    , a.mhs202_uniq_id
    , a.uniq_care_act_id
    , a.care_act_id
    , a.uniq_serv_req_id as referral_source_record_id
    , a.uniq_care_cont_id
    , a.care_contact_id
    , a.person_id
    , b.sk_patient_id
    , c.care_cont_date as care_activity_date
    , c.care_cont_time as care_activity_time
    , case
        when c.care_cont_date is null then null
        when c.care_cont_time is null then c.care_cont_date::timestamp_ntz
        else timestamp_ntz_from_parts(c.care_cont_date, c.care_cont_time)
    end as care_activity_at
    , case
        when c.care_cont_date is null then null
        when c.care_cont_time is null then 'date'
        else 'timestamp'
    end as care_activity_time_precision
    , iff(c.mhs201_uniq_id is not null, 'same_submission_care_contact', null)
        as care_activity_time_basis
    , a.clinical_contact_duration_minutes
    , coalesce(a.clinical_contact_duration_minutes > 1440, false)
        as is_clinical_contact_duration_over_24_hours
    , a.procedure_code
    , procedure.preferred_term as procedure_description
    , case
        when a.procedure_code is null then 'code_missing'
        when procedure.snomed_code is null then 'code_or_expression_unmatched'
        else 'labelled'
    end as procedure_label_status
    , a.finding_scheme_code
    , finding_scheme.description as finding_scheme_description
    , a.finding_code
    , case
        when a.finding_scheme_code = '01' then icd10_finding.description
        when a.finding_scheme_code = '04' then snomed_finding.preferred_term
    end as finding_description
    , case
        when a.finding_code is null then 'code_missing'
        when a.finding_scheme_code = '01' and icd10_finding.code is not null then 'labelled'
        when a.finding_scheme_code = '04' and snomed_finding.snomed_code is not null
            then 'labelled'
        when a.finding_scheme_code in ('02', '03')
            and mapped_snomed_finding.snomed_code is not null then 'mapped_to_snomed'
        when a.finding_scheme_code = '05' then 'reference_not_available'
        else 'code_unmatched'
    end as finding_label_status
    , a.standardised_snomed_finding_code
    , mapped_snomed_finding.preferred_term as standardised_snomed_finding_description
    , a.observation_scheme_code
    , a.is_observation_scheme_inferred
    , observation_scheme.description as observation_scheme_description
    , a.observation_code
    , observation.preferred_term as observation_description
    , case
        when a.observation_code is null then 'code_missing'
        when a.observation_scheme_code = '03' and observation.snomed_code is not null
            then 'labelled'
        when a.observation_scheme_code in ('01', '02')
            and mapped_snomed_observation.snomed_code is not null then 'mapped_to_snomed'
        when a.observation_scheme_code is null and observation.snomed_code is not null
            then 'labelled_snomed_scheme_missing'
        else 'code_unmatched'
    end as observation_label_status
    , a.standardised_snomed_observation_code
    , mapped_snomed_observation.preferred_term
        as standardised_snomed_observation_description
    , a.observation_value
    , a.unit_of_measurement_code
    , unit_of_measurement.description as unit_of_measurement_description
    , unit_of_measurement.unit_symbol as unit_of_measurement_symbol
    , unit_of_measurement.match_type as unit_of_measurement_match_type
    , unit_of_measurement.definition_source as unit_of_measurement_definition_source
    , case
        when a.unit_of_measurement_code is null then 'code_missing'
        when unit_of_measurement.code is null then 'code_unmatched'
        else 'labelled'
    end as unit_of_measurement_label_status
    , a.procedure_code is not null as has_procedure
    , a.finding_code is not null as has_finding
    , a.observation_code is not null or a.observation_value is not null as has_observation
    , (has_procedure::integer + has_finding::integer + has_observation::integer)
        as clinical_item_count
    , a.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , c.org_id_comm as commissioner_organisation_code
    , commissioner.organisation_name as commissioner_organisation_name
    , c.dm_icb_commissioner as derived_icb_commissioner_code
    , derived_icb.organisation_name as derived_icb_commissioner_name
    , c.dm_sub_icb_commissioner as derived_sub_icb_commissioner_code
    , derived_sub_icb.organisation_name as derived_sub_icb_commissioner_name
    , c.site_id_of_treat as treatment_site_code
    , coalesce(
        treatment_site.organisation_name
        , treatment_service_provider.service_provider_name
    ) as treatment_site_name
    , coalesce(
        c.uniq_other_care_prof_team_local_id
        , c.uniq_care_prof_team_id
    ) as service_or_team_id
    , coalesce(
        c.other_care_prof_team_local_id
        , c.care_prof_team_local_id
    ) as service_or_team_local_id
    , td.serv_team_type_mh as service_or_team_type_code
    , team_type.description as service_or_team_type_description
    , td.service_type_name as source_service_or_team_type_name
    , td.serv_team_int_age_group as service_or_team_intended_age_group_code
    , intended_age_group.description as service_or_team_intended_age_group_description
    , c.mhs201_uniq_id is not null as is_care_contact_linked
    , iff(
        c.mhs201_uniq_id is null
        , null
        , a.person_id is not distinct from c.person_id
    ) as is_care_contact_person_consistent
    , a.uniq_submission_id
    , a.uniq_month_id
    , a.reporting_period_start_date
    , a.reporting_period_end_date
    , a.mhsds_version
    , a.source_file_received_at
    , a.source_loaded_at
from {{ ref('stg_mhsds_care_activity') }} as a
left join {{ ref('stg_mhsds_carecontact') }} as c
    on a.uniq_submission_id = c.uniq_submission_id
    and a.uniq_serv_req_id = c.uniq_serv_req_id
    and a.uniq_care_cont_id = c.uniq_care_cont_id
left join {{ ref('stg_mhsds_bridging') }} as b
    on a.person_id = b.person_id
left join {{ ref('stg_dictionary_snomed_concept') }} as procedure
    on trim(a.procedure_code) = procedure.snomed_code
left join {{ ref('mhsds_care_activity_code_lookup') }} as finding_scheme
    on upper(trim(a.finding_scheme_code)) = finding_scheme.code
    and finding_scheme.code_set_name = 'finding_scheme'
left join {{ ref('stg_dictionary_dbo_diagnosis') }} as icd10_finding
    on {{ clean_icd10_code('upper(trim(a.finding_code))') }} = upper(icd10_finding.code)
    and a.finding_scheme_code = '01'
left join {{ ref('stg_dictionary_snomed_concept') }} as snomed_finding
    on trim(a.finding_code) = snomed_finding.snomed_code
    and a.finding_scheme_code = '04'
left join {{ ref('stg_dictionary_snomed_concept') }} as mapped_snomed_finding
    on trim(a.standardised_snomed_finding_code) = mapped_snomed_finding.snomed_code
left join {{ ref('mhsds_care_activity_code_lookup') }} as observation_scheme
    on upper(trim(a.observation_scheme_code)) = observation_scheme.code
    and observation_scheme.code_set_name = 'observation_scheme'
left join {{ ref('stg_dictionary_snomed_concept') }} as observation
    on trim(a.observation_code) = observation.snomed_code
left join {{ ref('stg_dictionary_snomed_concept') }} as mapped_snomed_observation
    on trim(a.standardised_snomed_observation_code)
        = mapped_snomed_observation.snomed_code
left join {{ ref('clinical_unit_of_measurement') }} as unit_of_measurement
    on trim(a.unit_of_measurement_code) = unit_of_measurement.code
left join {{ ref('stg_mhsds_service_or_team_details') }} as td
    on coalesce(c.other_care_prof_team_local_id, c.care_prof_team_local_id)
        = td.care_prof_team_local_id
    and a.uniq_submission_id = td.uniq_submission_id
left join {{ ref('mhsds_service_or_team_type') }} as team_type
    on upper(trim(td.serv_team_type_mh)) = team_type.code
left join {{ ref('mhsds_service_or_team_intended_age_group') }} as intended_age_group
    on upper(trim(td.serv_team_int_age_group)) = intended_age_group.code
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(a.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as commissioner
    on upper(c.org_id_comm) = upper(commissioner.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as derived_icb
    on upper(c.dm_icb_commissioner) = upper(derived_icb.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as derived_sub_icb
    on upper(c.dm_sub_icb_commissioner) = upper(derived_sub_icb.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as treatment_site
    on upper(trim(c.site_id_of_treat)) = upper(trim(treatment_site.organisation_code))
left join {{ ref('stg_dictionary_dbo_serviceprovider') }} as treatment_service_provider
    on upper(trim(c.site_id_of_treat))
        = upper(trim(treatment_service_provider.service_provider_code))
