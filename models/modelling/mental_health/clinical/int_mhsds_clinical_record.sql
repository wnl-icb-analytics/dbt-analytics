with existing_records as (
select
    d.source_record_id as source_record_id
    , d.source_row_id as source_row_id
    , d.source_table as source_table
    , d.clinical_record_type as clinical_record_type
    , d.person_id as person_id
    , d.local_patient_id as local_patient_id
    , d.referral_source_record_id as referral_source_record_id
    , null::varchar as uniq_care_cont_id
    , null::varchar as care_activity_source_record_id
    , null::varchar as uniq_care_act_id
    , null::varchar as care_prof_local_id
    , null::varchar as uniq_care_prof_local_id
    , d.clinical_at as clinical_at
    , d.clinical_time_precision as clinical_time_precision
    , d.clinical_time_basis as clinical_time_basis
    , d.source_timestamp as source_timestamp
    , d.source_derived_date as source_derived_date
    , d.is_source_date_inconsistent as is_source_date_inconsistent
    , d.diagnosis_scheme_code as coding_scheme_code
    , 'diagnosis' as coding_scheme_kind
    , null::varchar as source_coding_scheme_description
    , d.clinical_code as clinical_code
    , null::varchar as source_clinical_description
    , null::varchar as source_clinical_label_status
    , d.standardised_snomed_code as standardised_snomed_code
    , null::varchar as source_standardised_snomed_description
    , null::varchar as clinical_value
    , null::varchar as unit_of_measurement_code
    , null::varchar as unit_of_measurement_description
    , null::varchar as unit_of_measurement_label_status
    , d.provider_organisation_code as provider_organisation_code
    , d.uniq_submission_id as uniq_submission_id
    , d.reporting_period_start_date as reporting_period_start_date
    , d.reporting_period_end_date as reporting_period_end_date
    , d.mhsds_version as mhsds_version
    , d.source_file_received_at as source_file_received_at
    , d.source_loaded_at as source_loaded_at
    , d.first_reported_period_end_date as first_reported_period_end_date
    , d.last_reported_period_end_date as last_reported_period_end_date
    , d.accepted_source_record_count as accepted_source_record_count
    , d.reported_period_count as reported_period_count
    , d.has_person_identifier_changed as has_person_identifier_changed
    , null::boolean as is_care_activity_linked
    , null::boolean as is_care_activity_person_consistent
    , null::boolean as is_care_activity_referral_consistent
    , null::boolean as is_care_activity_contact_consistent
    , null::boolean as is_care_contact_person_consistent
from {{ ref('int_mhsds_diagnosis') }} as d

union all

select
    {{ dbt_utils.generate_surrogate_key(["'MHS606'", 'a.uniq_submission_id', 'a.mhs606_uniq_id']) }} as source_record_id
    , a.mhs606_uniq_id::varchar as source_row_id
    , 'MHS606' as source_table
    , 'referral_assessment' as clinical_record_type
    , a.person_id as person_id
    , null::varchar as local_patient_id
    , a.uniq_serv_req_id as referral_source_record_id
    , null::varchar as uniq_care_cont_id
    , null::varchar as care_activity_source_record_id
    , null::varchar as uniq_care_act_id
    , a.care_prof_local_id as care_prof_local_id
    , a.uniq_care_prof_local_id as uniq_care_prof_local_id
    , coalesce(
        iff(a.ass_tool_comp_timestamp::date >= '1901-01-01'::date, a.ass_tool_comp_timestamp, null)
        , iff(a.ass_tool_comp_date::date >= '1901-01-01'::date, a.ass_tool_comp_date::date::timestamp_ntz, null)
    ) as clinical_at
    , case
        when a.ass_tool_comp_timestamp::date >= '1901-01-01'::date
            then 'stored_timestamp_precision_unknown'
        when a.ass_tool_comp_date::date >= '1901-01-01'::date then 'date'
    end as clinical_time_precision
    , 'assessment_completion' as clinical_time_basis
    , a.ass_tool_comp_timestamp as source_timestamp
    , a.ass_tool_comp_date::date as source_derived_date
    , coalesce(a.ass_tool_comp_timestamp::date <> a.ass_tool_comp_date::date, false) as is_source_date_inconsistent
    , null::varchar as coding_scheme_code
    , 'fixed_snomed' as coding_scheme_kind
    , 'SNOMED CT' as source_coding_scheme_description
    , a.coded_ass_tool_type as clinical_code
    , null::varchar as source_clinical_description
    , null::varchar as source_clinical_label_status
    , null::varchar as standardised_snomed_code
    , null::varchar as source_standardised_snomed_description
    , a.pers_score as clinical_value
    , null::varchar as unit_of_measurement_code
    , null::varchar as unit_of_measurement_description
    , null::varchar as unit_of_measurement_label_status
    , a.org_id_prov as provider_organisation_code
    , a.uniq_submission_id as uniq_submission_id
    , a.reporting_period_start_date::date as reporting_period_start_date
    , a.reporting_period_end_date::date as reporting_period_end_date
    , a.dmic_dataset as mhsds_version
    , a.effective_from as source_file_received_at
    , a.dmic_date_added as source_loaded_at
    , a.reporting_period_end_date::date as first_reported_period_end_date
    , a.reporting_period_end_date::date as last_reported_period_end_date
    , 1 as accepted_source_record_count
    , 1 as reported_period_count
    , false as has_person_identifier_changed
    , null::boolean as is_care_activity_linked
    , null::boolean as is_care_activity_person_consistent
    , null::boolean as is_care_activity_referral_consistent
    , null::boolean as is_care_activity_contact_consistent
    , null::boolean as is_care_contact_person_consistent
from {{ ref('stg_mhsds_referral_assessment') }} as a

union all

select
    {{ dbt_utils.generate_surrogate_key(["'MHS607'", 'a.uniq_submission_id', 'a.mhs607_uniq_id']) }} as source_record_id
    , a.mhs607_uniq_id::varchar as source_row_id
    , 'MHS607' as source_table
    , 'activity_assessment' as clinical_record_type
    , a.person_id as person_id
    , null::varchar as local_patient_id
    , a.uniq_serv_req_id as referral_source_record_id
    , a.uniq_care_cont_id as uniq_care_cont_id
    , {{ dbt_utils.generate_surrogate_key(['a.org_id_prov', 'a.reporting_period_end_date::date', 'a.uniq_care_act_id']) }} as care_activity_source_record_id
    , a.uniq_care_act_id as uniq_care_act_id
    , null::varchar as care_prof_local_id
    , null::varchar as uniq_care_prof_local_id
    , iff(
        c.source_record_id is not null
            and a.person_id is not distinct from c.person_id
            and a.uniq_serv_req_id is not distinct from c.referral_source_record_id
        , c.care_activity_at, null
    ) as clinical_at
    , iff(
        c.source_record_id is not null
            and a.person_id is not distinct from c.person_id
            and a.uniq_serv_req_id is not distinct from c.referral_source_record_id
        , c.care_activity_time_precision, null
    ) as clinical_time_precision
    , iff(
        c.source_record_id is not null
            and a.person_id is not distinct from c.person_id
            and a.uniq_serv_req_id is not distinct from c.referral_source_record_id
        , iff(c.care_activity_time_basis = 'source_derived_activity_date',
            'source_derived_activity_date', 'same_submission_care_activity_contact'), null
    ) as clinical_time_basis
    , null::timestamp_ntz as source_timestamp
    , null::date as source_derived_date
    , false as is_source_date_inconsistent
    , null::varchar as coding_scheme_code
    , 'fixed_snomed' as coding_scheme_kind
    , 'SNOMED CT' as source_coding_scheme_description
    , a.coded_ass_tool_type as clinical_code
    , null::varchar as source_clinical_description
    , null::varchar as source_clinical_label_status
    , null::varchar as standardised_snomed_code
    , null::varchar as source_standardised_snomed_description
    , a.pers_score as clinical_value
    , null::varchar as unit_of_measurement_code
    , null::varchar as unit_of_measurement_description
    , null::varchar as unit_of_measurement_label_status
    , a.org_id_prov as provider_organisation_code
    , a.uniq_submission_id as uniq_submission_id
    , a.reporting_period_start_date::date as reporting_period_start_date
    , a.reporting_period_end_date::date as reporting_period_end_date
    , a.dmic_dataset as mhsds_version
    , a.effective_from as source_file_received_at
    , a.dmic_date_added as source_loaded_at
    , a.reporting_period_end_date::date as first_reported_period_end_date
    , a.reporting_period_end_date::date as last_reported_period_end_date
    , 1 as accepted_source_record_count
    , 1 as reported_period_count
    , false as has_person_identifier_changed
    , c.source_record_id is not null as is_care_activity_linked
    , iff(c.source_record_id is null, null, a.person_id is not distinct from c.person_id) as is_care_activity_person_consistent
    , iff(c.source_record_id is null, null, a.uniq_serv_req_id is not distinct from c.referral_source_record_id) as is_care_activity_referral_consistent
    , iff(c.source_record_id is null, null, a.uniq_care_cont_id is not distinct from c.uniq_care_cont_id) as is_care_activity_contact_consistent
    , c.is_care_contact_person_consistent as is_care_contact_person_consistent
from {{ ref('stg_mhsds_activity_assessment') }} as a
left join {{ ref('fct_mhsds_care_activity') }} as c
    on a.uniq_submission_id = c.uniq_submission_id
    and a.org_id_prov = c.provider_organisation_code
    and a.reporting_period_end_date::date = c.reporting_period_end_date
    and a.uniq_care_act_id = c.uniq_care_act_id

union all

select
    a.source_record_id as source_record_id
    , a.mhs202_uniq_id::varchar as source_row_id
    , 'MHS202' as source_table
    , 'procedure' as clinical_record_type
    , a.person_id as person_id
    , null::varchar as local_patient_id
    , a.referral_source_record_id as referral_source_record_id
    , a.uniq_care_cont_id as uniq_care_cont_id
    , a.source_record_id as care_activity_source_record_id
    , a.uniq_care_act_id as uniq_care_act_id
    , null::varchar as care_prof_local_id
    , null::varchar as uniq_care_prof_local_id
    , a.care_activity_at as clinical_at
    , a.care_activity_time_precision as clinical_time_precision
    , a.care_activity_time_basis as clinical_time_basis
    , null::timestamp_ntz as source_timestamp
    , null::date as source_derived_date
    , false as is_source_date_inconsistent
    , null::varchar as coding_scheme_code
    , 'fixed_snomed' as coding_scheme_kind
    , 'SNOMED CT' as source_coding_scheme_description
    , a.procedure_code as clinical_code
    , a.procedure_description as source_clinical_description
    , a.procedure_label_status as source_clinical_label_status
    , null::varchar as standardised_snomed_code
    , null::varchar as source_standardised_snomed_description
    , null::varchar as clinical_value
    , null::varchar as unit_of_measurement_code
    , null::varchar as unit_of_measurement_description
    , null::varchar as unit_of_measurement_label_status
    , a.provider_organisation_code as provider_organisation_code
    , a.uniq_submission_id as uniq_submission_id
    , a.reporting_period_start_date as reporting_period_start_date
    , a.reporting_period_end_date as reporting_period_end_date
    , a.mhsds_version as mhsds_version
    , a.source_file_received_at as source_file_received_at
    , a.source_loaded_at as source_loaded_at
    , a.reporting_period_end_date as first_reported_period_end_date
    , a.reporting_period_end_date as last_reported_period_end_date
    , 1 as accepted_source_record_count
    , 1 as reported_period_count
    , false as has_person_identifier_changed
    , true as is_care_activity_linked
    , true as is_care_activity_person_consistent
    , true as is_care_activity_referral_consistent
    , true as is_care_activity_contact_consistent
    , a.is_care_contact_person_consistent as is_care_contact_person_consistent
from {{ ref('fct_mhsds_care_activity') }} as a
where a.has_procedure

union all

select
    a.source_record_id as source_record_id
    , a.mhs202_uniq_id::varchar as source_row_id
    , 'MHS202' as source_table
    , 'finding' as clinical_record_type
    , a.person_id as person_id
    , null::varchar as local_patient_id
    , a.referral_source_record_id as referral_source_record_id
    , a.uniq_care_cont_id as uniq_care_cont_id
    , a.source_record_id as care_activity_source_record_id
    , a.uniq_care_act_id as uniq_care_act_id
    , null::varchar as care_prof_local_id
    , null::varchar as uniq_care_prof_local_id
    , a.care_activity_at as clinical_at
    , a.care_activity_time_precision as clinical_time_precision
    , a.care_activity_time_basis as clinical_time_basis
    , null::timestamp_ntz as source_timestamp
    , null::date as source_derived_date
    , false as is_source_date_inconsistent
    , a.finding_scheme_code as coding_scheme_code
    , 'finding' as coding_scheme_kind
    , a.finding_scheme_description as source_coding_scheme_description
    , a.finding_code as clinical_code
    , a.finding_description as source_clinical_description
    , a.finding_label_status as source_clinical_label_status
    , a.standardised_snomed_finding_code as standardised_snomed_code
    , a.standardised_snomed_finding_description as source_standardised_snomed_description
    , null::varchar as clinical_value
    , null::varchar as unit_of_measurement_code
    , null::varchar as unit_of_measurement_description
    , null::varchar as unit_of_measurement_label_status
    , a.provider_organisation_code as provider_organisation_code
    , a.uniq_submission_id as uniq_submission_id
    , a.reporting_period_start_date as reporting_period_start_date
    , a.reporting_period_end_date as reporting_period_end_date
    , a.mhsds_version as mhsds_version
    , a.source_file_received_at as source_file_received_at
    , a.source_loaded_at as source_loaded_at
    , a.reporting_period_end_date as first_reported_period_end_date
    , a.reporting_period_end_date as last_reported_period_end_date
    , 1 as accepted_source_record_count
    , 1 as reported_period_count
    , false as has_person_identifier_changed
    , true as is_care_activity_linked
    , true as is_care_activity_person_consistent
    , true as is_care_activity_referral_consistent
    , true as is_care_activity_contact_consistent
    , a.is_care_contact_person_consistent as is_care_contact_person_consistent
from {{ ref('fct_mhsds_care_activity') }} as a
where a.has_finding

union all

select
    a.source_record_id as source_record_id
    , a.mhs202_uniq_id::varchar as source_row_id
    , 'MHS202' as source_table
    , 'observation' as clinical_record_type
    , a.person_id as person_id
    , null::varchar as local_patient_id
    , a.referral_source_record_id as referral_source_record_id
    , a.uniq_care_cont_id as uniq_care_cont_id
    , a.source_record_id as care_activity_source_record_id
    , a.uniq_care_act_id as uniq_care_act_id
    , null::varchar as care_prof_local_id
    , null::varchar as uniq_care_prof_local_id
    , a.care_activity_at as clinical_at
    , a.care_activity_time_precision as clinical_time_precision
    , a.care_activity_time_basis as clinical_time_basis
    , null::timestamp_ntz as source_timestamp
    , null::date as source_derived_date
    , false as is_source_date_inconsistent
    , iff(a.is_observation_scheme_inferred, null, a.observation_scheme_code) as coding_scheme_code
    , iff(a.is_observation_scheme_inferred, 'fixed_snomed', 'observation') as coding_scheme_kind
    , a.observation_scheme_description as source_coding_scheme_description
    , a.observation_code as clinical_code
    , a.observation_description as source_clinical_description
    , a.observation_label_status as source_clinical_label_status
    , a.standardised_snomed_observation_code as standardised_snomed_code
    , a.standardised_snomed_observation_description as source_standardised_snomed_description
    , a.observation_value::varchar as clinical_value
    , a.unit_of_measurement_code as unit_of_measurement_code
    , a.unit_of_measurement_description as unit_of_measurement_description
    , a.unit_of_measurement_label_status as unit_of_measurement_label_status
    , a.provider_organisation_code as provider_organisation_code
    , a.uniq_submission_id as uniq_submission_id
    , a.reporting_period_start_date as reporting_period_start_date
    , a.reporting_period_end_date as reporting_period_end_date
    , a.mhsds_version as mhsds_version
    , a.source_file_received_at as source_file_received_at
    , a.source_loaded_at as source_loaded_at
    , a.reporting_period_end_date as first_reported_period_end_date
    , a.reporting_period_end_date as last_reported_period_end_date
    , 1 as accepted_source_record_count
    , 1 as reported_period_count
    , false as has_person_identifier_changed
    , true as is_care_activity_linked
    , true as is_care_activity_person_consistent
    , true as is_care_activity_referral_consistent
    , true as is_care_activity_contact_consistent
    , a.is_care_contact_person_consistent as is_care_contact_person_consistent
from {{ ref('fct_mhsds_care_activity') }} as a
where a.has_observation

)
select
    e.*
    , null::varchar as source_assessment_id
    , null::boolean as is_assessment_parent_linked
    , null::boolean as is_assessment_parent_person_consistent
from existing_records as e

union all

select
    a.source_record_id as source_record_id
    , a.mhs802_uniq_id::varchar as source_row_id
    , 'MHS802' as source_table
    , 'clustering_assessment' as clinical_record_type
    , a.person_id as person_id
    , null::varchar as local_patient_id
    , null::varchar as referral_source_record_id
    , null::varchar as uniq_care_cont_id
    , null::varchar as care_activity_source_record_id
    , null::varchar as uniq_care_act_id
    , null::varchar as care_prof_local_id
    , null::varchar as uniq_care_prof_local_id
    , iff(a.assessment_time is null, a.assessment_date::timestamp_ntz, timestamp_ntz_from_parts(a.assessment_date,a.assessment_time)) as clinical_at
    , iff(a.assessment_date is null, null, iff(a.assessment_time is null,'date','timestamp')) as clinical_time_precision
    , 'same_submission_clustering_completion' as clinical_time_basis
    , null::timestamp_ntz as source_timestamp
    , a.assessment_date as source_derived_date
    , null::boolean as is_source_date_inconsistent
    , null::varchar as coding_scheme_code
    , 'fixed_snomed' as coding_scheme_kind
    , 'SNOMED CT' as source_coding_scheme_description
    , a.coded_ass_tool_type as clinical_code
    , null::varchar as source_clinical_description
    , null::varchar as source_clinical_label_status
    , null::varchar as standardised_snomed_code
    , null::varchar as source_standardised_snomed_description
    , a.pers_score as clinical_value
    , null::varchar as unit_of_measurement_code
    , null::varchar as unit_of_measurement_description
    , null::varchar as unit_of_measurement_label_status
    , a.org_id_prov as provider_organisation_code
    , a.uniq_submission_id as uniq_submission_id
    , a.reporting_period_start_date as reporting_period_start_date
    , a.reporting_period_end_date as reporting_period_end_date
    , a.dmic_dataset as mhsds_version
    , a.effective_from as source_file_received_at
    , null::timestamp_ntz as source_loaded_at
    , a.first_reported_period_end_date as first_reported_period_end_date
    , a.last_reported_period_end_date as last_reported_period_end_date
    , a.accepted_source_record_count as accepted_source_record_count
    , a.reported_period_count as reported_period_count
    , a.has_person_identifier_changed as has_person_identifier_changed
    , null::boolean as is_care_activity_linked
    , null::boolean as is_care_activity_person_consistent
    , null::boolean as is_care_activity_referral_consistent
    , null::boolean as is_care_activity_contact_consistent
    , null::boolean as is_care_contact_person_consistent
    , a.uniq_clust_id as source_assessment_id
    , a.is_assessment_parent_linked as is_assessment_parent_linked
    , a.is_assessment_parent_person_consistent as is_assessment_parent_person_consistent
from {{ ref('int_mhsds_clustering_assessment_response') }} as a

union all

select
    a.source_record_id as source_record_id
    , a.mhs609_uniq_id::varchar as source_row_id
    , 'MHS609' as source_table
    , 'presenting_complaint' as clinical_record_type
    , a.person_id as person_id
    , null::varchar as local_patient_id
    , a.uniq_serv_req_id as referral_source_record_id
    , null::varchar as uniq_care_cont_id
    , null::varchar as care_activity_source_record_id
    , null::varchar as uniq_care_act_id
    , null::varchar as care_prof_local_id
    , null::varchar as uniq_care_prof_local_id
    , a.pres_comp_date::timestamp_ntz as clinical_at
    , iff(a.pres_comp_date is not null,'date',null) as clinical_time_precision
    , 'presenting_complaint_recorded' as clinical_time_basis
    , null::timestamp_ntz as source_timestamp
    , a.pres_comp_date as source_derived_date
    , null::boolean as is_source_date_inconsistent
    , a.find_scheme_in_use as coding_scheme_code
    , 'finding' as coding_scheme_kind
    , a.finding_scheme_description as source_coding_scheme_description
    , a.pres_comp as clinical_code
    , a.complaint_description as source_clinical_description
    , case when a.pres_comp is null then 'code_missing' when a.complaint_description is not null then 'labelled' when a.finding_scheme_description is null then 'coding_scheme_unrecognised' else 'code_unmatched' end as source_clinical_label_status
    , null::varchar as standardised_snomed_code
    , null::varchar as source_standardised_snomed_description
    , null::varchar as clinical_value
    , null::varchar as unit_of_measurement_code
    , null::varchar as unit_of_measurement_description
    , null::varchar as unit_of_measurement_label_status
    , a.org_id_prov as provider_organisation_code
    , a.uniq_submission_id as uniq_submission_id
    , a.reporting_period_start_date as reporting_period_start_date
    , a.reporting_period_end_date as reporting_period_end_date
    , a.dmic_dataset as mhsds_version
    , a.effective_from as source_file_received_at
    , null::timestamp_ntz as source_loaded_at
    , a.first_reported_period_end_date as first_reported_period_end_date
    , a.last_reported_period_end_date as last_reported_period_end_date
    , a.accepted_source_record_count as accepted_source_record_count
    , a.reported_period_count as reported_period_count
    , false as has_person_identifier_changed
    , null::boolean as is_care_activity_linked
    , null::boolean as is_care_activity_person_consistent
    , null::boolean as is_care_activity_referral_consistent
    , null::boolean as is_care_activity_contact_consistent
    , null::boolean as is_care_contact_person_consistent
    , null::varchar as source_assessment_id
    , null::boolean as is_assessment_parent_linked
    , null::boolean as is_assessment_parent_person_consistent
from {{ ref('int_mhsds_presenting_complaint') }} as a
