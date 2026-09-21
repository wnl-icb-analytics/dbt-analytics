with activity_context as (
    select
        a.*
        , r.mhs101_uniq_id as same_submission_referral_record_id
        , a.person_id = r.person_id as is_same_submission_referral_person_consistent
        , {{ mhsds_delivering_team_basis(
            'a.other_care_prof_team_local_id', 'a.care_prof_team_local_id',
            'r.care_prof_team_local_id', 's.dat_set_ver'
        ) }} as service_or_team_attribution_basis
        , case service_or_team_attribution_basis
            when 'contact_additional_team' then a.other_care_prof_team_local_id
            when 'contact_legacy_team' then a.care_prof_team_local_id
            when 'referral_primary_team' then r.care_prof_team_local_id
        end as service_or_team_local_id
        , iff(service_or_team_attribution_basis = 'referral_primary_team',
            r.serv_team_type, null) as primary_team_type_code
    from {{ ref('stg_mhsds_indirectactivity') }} as a
    left join {{ ref('stg_mhsds_referral_history') }} as r
        on a.uniq_submission_id = r.uniq_submission_id and a.uniq_serv_req_id = r.uniq_serv_req_id
    left join {{ ref('stg_mhsds_activesubmission') }} as s on a.uniq_submission_id = s.uniq_submission_id
)
select
    a.mhs204_uniq_id as indirect_activity_id
    , a.person_id
    , b.sk_patient_id
    , a.uniq_serv_req_id as referral_source_record_id
    , {{ dbt_utils.generate_surrogate_key(['a.uniq_submission_id','a.uniq_serv_req_id']) }} as referral_period_id
    , {{ dbt_utils.generate_surrogate_key(['a.person_id','a.org_id_prov','a.reporting_period_end_date']) }} as person_provider_period_id
    , a.indirect_act_date as indirect_activity_date
    , a.indirect_act_time as indirect_activity_time
    , a.duration_indirect_act as duration_minutes
    , coalesce(a.duration_indirect_act < 0 or a.duration_indirect_act > 1440, false) as is_duration_outside_one_day
    , a.ind_act_pers_cons as person_consulted_code
    , consulted.description as person_consulted_description
    , a.ind_act_procedure as procedure_code
    , procedure.preferred_term as procedure_description
    , a.find_scheme_in_use as finding_scheme_code
    , scheme.description as finding_scheme_description
    , a.finding as finding_code
    , case a.find_scheme_in_use
        when '01' then icd.description when '04' then finding.preferred_term
    end as finding_description
    , a.master_snomed_ct_finding_code as source_standardised_snomed_finding_code
    , mapped.preferred_term as source_standardised_snomed_finding_description
    , a.care_prof_local_id as care_professional_local_id
    , a.uniq_care_prof_local_id as care_professional_id
    , a.service_or_team_local_id
    , coalesce(a.primary_team_type_code, team.serv_team_type_mh) as service_or_team_type_code
    , team_type.description as service_or_team_type_description
    , a.service_or_team_attribution_basis
    , a.same_submission_referral_record_id is not null as is_same_submission_referral_linked
    , a.is_same_submission_referral_person_consistent
    , a.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , a.org_id_comm as commissioner_organisation_code
    , commissioner.organisation_name as commissioner_organisation_name
    , a.dm_icb_commissioner as source_derived_icb_commissioner_code
    , icb.organisation_name as source_derived_icb_commissioner_name
    , a.dm_sub_icb_commissioner as source_derived_sub_icb_commissioner_code
    , subicb.organisation_name as source_derived_sub_icb_commissioner_name
    , a.reporting_period_start_date
    , a.reporting_period_end_date
    , a.uniq_submission_id as submission_id
    , a.effective_from as source_file_received_at
from activity_context as a
left join {{ ref('stg_mhsds_bridging') }} as b on a.person_id = b.person_id
left join {{ ref('mhsds_domain_code_lookup') }} as consulted
    on a.ind_act_pers_cons = consulted.code and consulted.code_set_name = 'indirect_activity_person_consulted'
left join {{ ref('snomed_concept') }} as procedure on trim(a.ind_act_procedure) = procedure.snomed_code
left join {{ ref('mhsds_care_activity_code_lookup') }} as scheme
    on a.find_scheme_in_use = scheme.code and scheme.code_set_name = 'finding_scheme'
left join {{ ref('icd10_code') }} as icd
    on replace({{ clean_icd10_code('upper(trim(a.finding))') }}, '.', '') = icd.code and a.find_scheme_in_use = '01'
left join {{ ref('snomed_concept') }} as finding on trim(a.finding) = finding.snomed_code and a.find_scheme_in_use = '04'
left join {{ ref('snomed_concept') }} as mapped on trim(a.master_snomed_ct_finding_code) = mapped.snomed_code
left join {{ ref('stg_mhsds_service_or_team_details') }} as team
    on a.uniq_submission_id = team.uniq_submission_id and a.service_or_team_local_id = team.care_prof_team_local_id
left join {{ ref('mhsds_service_or_team_type') }} as team_type
    on coalesce(a.primary_team_type_code, team.serv_team_type_mh) = team_type.code
left join {{ ref('int_mhsds_organisation') }} as provider on upper(a.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as commissioner on upper(a.org_id_comm) = upper(commissioner.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as icb on upper(a.dm_icb_commissioner) = upper(icb.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as subicb on upper(a.dm_sub_icb_commissioner) = upper(subicb.organisation_code)
