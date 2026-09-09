select
    i.source_record_id as originating_source_record_id
    , i.cyp501_unique_id::varchar as source_row_id
    , 'CYP501' as source_table
    , 'immunisation' as clinical_record_type
    , i.person_id as person_id
    , i.organisation_code_provider as provider_organisation_code
    , i.local_patient_identifier_extended as local_patient_id
    , i.organisation_code_immunisation_responsible_organisation as immunisation_responsible_organisation_code
    , null::varchar as referral_source_record_id
    , null::varchar as care_activity_source_record_id
    , null::varchar as unique_care_activity_identifier
    , null::varchar as unique_care_contact_identifier
    , i.immunisation_date::date::timestamp_ntz as clinical_at
    , iff(i.immunisation_date is not null, 'date', null) as clinical_time_precision
    , 'immunisation_administration' as clinical_time_basis
    , i.immunisation_date::date as source_clinical_date
    , null::date as source_derived_date
    , 'procedure' as coding_scheme_kind
    , i.coding_scheme_code as coding_scheme_code
    , i.immunisation_procedure_clinical_terminology as clinical_code
    , null::varchar as clinical_value
    , null::varchar as unit_of_measurement_code
    , i.unique_submission_id as unique_submission_id
    , i.reporting_period_start_date as reporting_period_start_date
    , i.reporting_period_end_date as reporting_period_end_date
    , i.effective_from as source_file_received_at
    , i.csds_version as csds_version
    , i.file_type as file_type
    , i.first_reported_period_end_date as first_reported_period_end_date
    , i.last_reported_period_end_date as last_reported_period_end_date
    , i.accepted_source_record_count as accepted_source_record_count
    , null::boolean as is_care_activity_linked
    , null::boolean as is_care_activity_person_consistent
    , null::boolean as is_submitted_contact_person_consistent
from {{ ref('int_csds_coded_immunisation') }} as i

union all

select
    {{ dbt_utils.generate_surrogate_key(["'CYP609'", 'r.unique_submission_id', 'r.cyp609_unique_id']) }} as originating_source_record_id
    , r.cyp609_unique_id::varchar as source_row_id
    , 'CYP609' as source_table
    , 'referral_assessment' as clinical_record_type
    , r.person_id as person_id
    , r.organisation_code_provider as provider_organisation_code
    , null::varchar as local_patient_id
    , null::varchar as immunisation_responsible_organisation_code
    , r.unique_service_request_identifier as referral_source_record_id
    , null::varchar as care_activity_source_record_id
    , null::varchar as unique_care_activity_identifier
    , null::varchar as unique_care_contact_identifier
    , r.assessment_tool_completion_date::date::timestamp_ntz as clinical_at
    , iff(r.assessment_tool_completion_date is not null, 'date', null) as clinical_time_precision
    , 'assessment_completion' as clinical_time_basis
    , r.assessment_tool_completion_date::date as source_clinical_date
    , null::date as source_derived_date
    , 'fixed_snomed' as coding_scheme_kind
    , null::varchar as coding_scheme_code
    , r.coded_assessment_tool_type_snomed_ct as clinical_code
    , r.person_score::varchar as clinical_value
    , null::varchar as unit_of_measurement_code
    , r.unique_submission_id as unique_submission_id
    , r.reporting_period_start_date as reporting_period_start_date
    , r.reporting_period_end_date as reporting_period_end_date
    , r.effective_from as source_file_received_at
    , r.csds_version as csds_version
    , r.file_type as file_type
    , r.reporting_period_end_date::date as first_reported_period_end_date
    , r.reporting_period_end_date::date as last_reported_period_end_date
    , 1::number as accepted_source_record_count
    , null::boolean as is_care_activity_linked
    , null::boolean as is_care_activity_person_consistent
    , null::boolean as is_submitted_contact_person_consistent
from {{ ref('stg_csds_referral_assessment') }} as r

union all

select
    {{ dbt_utils.generate_surrogate_key(["'CYP612'", 'r.unique_submission_id', 'r.cyp612_unique_id']) }} as originating_source_record_id
    , r.cyp612_unique_id::varchar as source_row_id
    , 'CYP612' as source_table
    , 'activity_assessment' as clinical_record_type
    , r.person_id as person_id
    , r.organisation_code_provider as provider_organisation_code
    , null::varchar as local_patient_id
    , null::varchar as immunisation_responsible_organisation_code
    , a.referral_id as referral_source_record_id
    , {{ dbt_utils.generate_surrogate_key(['r.unique_submission_id', 'r.unique_care_activity_identifier']) }} as care_activity_source_record_id
    , r.unique_care_activity_identifier as unique_care_activity_identifier
    , a.contact_id as unique_care_contact_identifier
    , iff(a.source_record_id is not null and r.person_id is not null and r.person_id = a.person_id and a.is_submitted_contact_person_consistent, a.care_contact_at, null) as clinical_at
    , iff(a.source_record_id is not null and r.person_id is not null and r.person_id = a.person_id and a.is_submitted_contact_person_consistent, a.care_contact_time_precision, null) as clinical_time_precision
    , iff(a.source_record_id is not null and r.person_id is not null and r.person_id = a.person_id and a.is_submitted_contact_person_consistent, 'same_submission_care_activity_contact', null) as clinical_time_basis
    , null::date as source_clinical_date
    , r.dmic_observation_date::date as source_derived_date
    , 'fixed_snomed' as coding_scheme_kind
    , null::varchar as coding_scheme_code
    , r.coded_assessment_tool_type_snomed_ct as clinical_code
    , r.person_score::varchar as clinical_value
    , null::varchar as unit_of_measurement_code
    , r.unique_submission_id as unique_submission_id
    , r.reporting_period_start_date as reporting_period_start_date
    , r.reporting_period_end_date as reporting_period_end_date
    , r.effective_from as source_file_received_at
    , r.csds_version as csds_version
    , r.file_type as file_type
    , r.reporting_period_end_date::date as first_reported_period_end_date
    , r.reporting_period_end_date::date as last_reported_period_end_date
    , 1::number as accepted_source_record_count
    , a.source_record_id is not null as is_care_activity_linked
    , iff(a.source_record_id is null or r.person_id is null or a.person_id is null, null, r.person_id = a.person_id) as is_care_activity_person_consistent
    , a.is_submitted_contact_person_consistent as is_submitted_contact_person_consistent
from {{ ref('stg_csds_activity_assessment') }} as r
left join {{ ref('fct_csds_care_activity') }} as a
    on r.unique_submission_id = a.submission_id
    and r.unique_care_activity_identifier = a.activity_id
    and r.organisation_code_provider = a.provider_organisation_code

union all

select
    a.source_record_id as originating_source_record_id
    , a.source_row_id::varchar as source_row_id
    , 'CYP202' as source_table
    , 'procedure' as clinical_record_type
    , a.person_id as person_id
    , a.provider_organisation_code as provider_organisation_code
    , null::varchar as local_patient_id
    , null::varchar as immunisation_responsible_organisation_code
    , a.referral_id as referral_source_record_id
    , a.source_record_id as care_activity_source_record_id
    , a.activity_id as unique_care_activity_identifier
    , a.contact_id as unique_care_contact_identifier
    , iff(a.is_submitted_contact_person_consistent, a.care_contact_at, null) as clinical_at
    , iff(a.is_submitted_contact_person_consistent, a.care_contact_time_precision, null) as clinical_time_precision
    , iff(a.is_submitted_contact_person_consistent, 'same_submission_care_contact', null) as clinical_time_basis
    , null::date as source_clinical_date
    , null::date as source_derived_date
    , 'procedure' as coding_scheme_kind
    , a.procedure_scheme_code as coding_scheme_code
    , a.procedure_code as clinical_code
    , null::varchar as clinical_value
    , null::varchar as unit_of_measurement_code
    , a.submission_id as unique_submission_id
    , a.reporting_period_start_date as reporting_period_start_date
    , a.reporting_period_end_date as reporting_period_end_date
    , a.source_file_received_at as source_file_received_at
    , a.csds_version as csds_version
    , a.file_type as file_type
    , a.reporting_period_end_date::date as first_reported_period_end_date
    , a.reporting_period_end_date::date as last_reported_period_end_date
    , 1::number as accepted_source_record_count
    , true as is_care_activity_linked
    , iff(a.person_id is null, null, true) as is_care_activity_person_consistent
    , a.is_submitted_contact_person_consistent as is_submitted_contact_person_consistent
from {{ ref('fct_csds_care_activity') }} as a
where a.procedure_code is not null

union all

select
    a.source_record_id as originating_source_record_id
    , a.source_row_id::varchar as source_row_id
    , 'CYP202' as source_table
    , 'finding' as clinical_record_type
    , a.person_id as person_id
    , a.provider_organisation_code as provider_organisation_code
    , null::varchar as local_patient_id
    , null::varchar as immunisation_responsible_organisation_code
    , a.referral_id as referral_source_record_id
    , a.source_record_id as care_activity_source_record_id
    , a.activity_id as unique_care_activity_identifier
    , a.contact_id as unique_care_contact_identifier
    , iff(a.is_submitted_contact_person_consistent, a.care_contact_at, null) as clinical_at
    , iff(a.is_submitted_contact_person_consistent, a.care_contact_time_precision, null) as clinical_time_precision
    , iff(a.is_submitted_contact_person_consistent, 'same_submission_care_contact', null) as clinical_time_basis
    , null::date as source_clinical_date
    , null::date as source_derived_date
    , 'finding' as coding_scheme_kind
    , a.finding_scheme_code as coding_scheme_code
    , a.finding_code as clinical_code
    , null::varchar as clinical_value
    , null::varchar as unit_of_measurement_code
    , a.submission_id as unique_submission_id
    , a.reporting_period_start_date as reporting_period_start_date
    , a.reporting_period_end_date as reporting_period_end_date
    , a.source_file_received_at as source_file_received_at
    , a.csds_version as csds_version
    , a.file_type as file_type
    , a.reporting_period_end_date::date as first_reported_period_end_date
    , a.reporting_period_end_date::date as last_reported_period_end_date
    , 1::number as accepted_source_record_count
    , true as is_care_activity_linked
    , iff(a.person_id is null, null, true) as is_care_activity_person_consistent
    , a.is_submitted_contact_person_consistent as is_submitted_contact_person_consistent
from {{ ref('fct_csds_care_activity') }} as a
where a.finding_code is not null

union all

select
    a.source_record_id as originating_source_record_id
    , a.source_row_id::varchar as source_row_id
    , 'CYP202' as source_table
    , 'observation' as clinical_record_type
    , a.person_id as person_id
    , a.provider_organisation_code as provider_organisation_code
    , null::varchar as local_patient_id
    , null::varchar as immunisation_responsible_organisation_code
    , a.referral_id as referral_source_record_id
    , a.source_record_id as care_activity_source_record_id
    , a.activity_id as unique_care_activity_identifier
    , a.contact_id as unique_care_contact_identifier
    , iff(a.is_submitted_contact_person_consistent, a.care_contact_at, null) as clinical_at
    , iff(a.is_submitted_contact_person_consistent, a.care_contact_time_precision, null) as clinical_time_precision
    , iff(a.is_submitted_contact_person_consistent, 'same_submission_care_contact', null) as clinical_time_basis
    , null::date as source_clinical_date
    , null::date as source_derived_date
    , 'observation' as coding_scheme_kind
    , a.observation_scheme_code as coding_scheme_code
    , a.observation_code as clinical_code
    , a.observation_value::varchar as clinical_value
    , a.observation_unit_code as unit_of_measurement_code
    , a.submission_id as unique_submission_id
    , a.reporting_period_start_date as reporting_period_start_date
    , a.reporting_period_end_date as reporting_period_end_date
    , a.source_file_received_at as source_file_received_at
    , a.csds_version as csds_version
    , a.file_type as file_type
    , a.reporting_period_end_date::date as first_reported_period_end_date
    , a.reporting_period_end_date::date as last_reported_period_end_date
    , 1::number as accepted_source_record_count
    , true as is_care_activity_linked
    , iff(a.person_id is null, null, true) as is_care_activity_person_consistent
    , a.is_submitted_contact_person_consistent as is_submitted_contact_person_consistent
from {{ ref('fct_csds_care_activity') }} as a
where a.observation_code is not null or a.observation_value is not null
