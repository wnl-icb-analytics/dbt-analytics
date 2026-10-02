with activities as (
    select
        *
        -- Submission and contact identifiers establish the link even when person IDs are absent.
        , is_submitted_contact_linked
            and is_submitted_contact_person_consistent is distinct from false as can_inherit_contact_time
    from {{ ref('fct_csds_care_activity') }}
)

, existing_records as (
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
    , iff(
        a.source_record_id is not null
            and (r.person_id is null or a.person_id is null or r.person_id = a.person_id)
            and a.can_inherit_contact_time
        , a.care_contact_at, null
    ) as clinical_at
    , iff(
        a.source_record_id is not null
            and (r.person_id is null or a.person_id is null or r.person_id = a.person_id)
            and a.can_inherit_contact_time
        , a.care_contact_time_precision, null
    ) as clinical_time_precision
    , iff(
        a.source_record_id is not null
            and (r.person_id is null or a.person_id is null or r.person_id = a.person_id)
            and a.can_inherit_contact_time
        , 'same_submission_care_activity_contact', null
    ) as clinical_time_basis
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
left join activities as a
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
    , iff(a.can_inherit_contact_time, a.care_contact_at, null) as clinical_at
    , iff(a.can_inherit_contact_time, a.care_contact_time_precision, null) as clinical_time_precision
    , iff(a.can_inherit_contact_time, 'same_submission_care_contact', null) as clinical_time_basis
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
from activities as a
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
    , iff(a.can_inherit_contact_time, a.care_contact_at, null) as clinical_at
    , iff(a.can_inherit_contact_time, a.care_contact_time_precision, null) as clinical_time_precision
    , iff(a.can_inherit_contact_time, 'same_submission_care_contact', null) as clinical_time_basis
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
from activities as a
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
    , iff(a.can_inherit_contact_time, a.care_contact_at, null) as clinical_at
    , iff(a.can_inherit_contact_time, a.care_contact_time_precision, null) as clinical_time_precision
    , iff(a.can_inherit_contact_time, 'same_submission_care_contact', null) as clinical_time_basis
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
from activities as a
where a.observation_code is not null or a.observation_value is not null
)

-- Restated sections reduced to their ETOS record keys in staging. Code-list sections repeat per coded field.
, submitted_items as (
select
    source_record_id, cyp502_unique_id::varchar as source_row_id, 'CYP502' as source_table
    , 'childhood_immunisation' as clinical_record_type, person_id, organisation_code_provider
    , local_patient_identifier_extended as local_patient_id
    , organisation_code_immunisation_responsible_organisation as immunisation_responsible_organisation_code
    , null::varchar as referral_source_record_id
    , immunisation_date as clinical_date, 'immunisation_administration' as clinical_time_basis
    , 'code_list' as coding_scheme_kind, null::varchar as coding_scheme_code
    , 'childhood_immunisation_type' as code_set_name, childhood_immunisation_type as clinical_code
    , unique_submission_id, reporting_period_start_date, reporting_period_end_date, effective_from, csds_version, file_type
    , first_reported_period_end_date, last_reported_period_end_date, accepted_source_record_count
from {{ ref('stg_csds_childhood_immunisation') }}

union all

select
    source_record_id, cyp601_unique_id::varchar, 'CYP601'
    , 'previous_diagnosis', person_id, organisation_code_provider
    , local_patient_identifier_extended, null::varchar, null::varchar
    , diagnosis_date, 'diagnosis_date'
    , 'diagnosis', diagnosis_scheme_code
    , null::varchar, diagnosis_code
    , unique_submission_id, reporting_period_start_date, reporting_period_end_date, effective_from, csds_version, file_type
    , first_reported_period_end_date, last_reported_period_end_date, accepted_source_record_count
from {{ ref('stg_csds_previous_diagnosis') }}

{% for model_name, source_table, record_type, time_basis in [
    ('stg_csds_provisional_diagnosis', 'CYP606', 'provisional_diagnosis', 'provisional_diagnosis_date'),
    ('stg_csds_primary_diagnosis', 'CYP607', 'primary_diagnosis', 'diagnosis_date'),
    ('stg_csds_secondary_diagnosis', 'CYP608', 'secondary_diagnosis', 'diagnosis_date')
] %}
union all

select
    source_record_id, {{ source_table | lower }}_unique_id::varchar, '{{ source_table }}'
    , '{{ record_type }}', person_id, organisation_code_provider
    , null::varchar, null::varchar, unique_service_request_identifier
    , diagnosis_date, '{{ time_basis }}'
    , 'diagnosis', diagnosis_scheme_code
    , null::varchar, diagnosis_code
    , unique_submission_id, reporting_period_start_date, reporting_period_end_date, effective_from, csds_version, file_type
    , first_reported_period_end_date, last_reported_period_end_date, accepted_source_record_count
from {{ ref(model_name) }}
{% endfor %}

union all

-- CSDS supplies no screening date; the audiology outcome takes the audiology procedure date.
select
    source_record_id, cyp603_unique_id::varchar, 'CYP603'
    , 'newborn_hearing_screening', person_id, organisation_code_provider
    , local_patient_identifier_extended, null::varchar, null::varchar
    , null::date, null::varchar
    , 'code_list', null::varchar
    , 'newborn_hearing_screening_outcome', newborn_hearing_screening_outcome
    , unique_submission_id, reporting_period_start_date, reporting_period_end_date, effective_from, csds_version, file_type
    , first_reported_period_end_date, last_reported_period_end_date, accepted_source_record_count
from {{ ref('stg_csds_newborn_hearing_screening') }}
where newborn_hearing_screening_outcome is not null

union all

select
    source_record_id, cyp603_unique_id::varchar, 'CYP603'
    , 'newborn_hearing_audiology', person_id, organisation_code_provider
    , local_patient_identifier_extended, null::varchar, null::varchar
    , audiology_procedure_date, 'audiology_procedure_date'
    , 'code_list', null::varchar
    , 'newborn_hearing_audiology_outcome', newborn_hearing_audiology_outcome
    , unique_submission_id, reporting_period_start_date, reporting_period_end_date, effective_from, csds_version, file_type
    , first_reported_period_end_date, last_reported_period_end_date, accepted_source_record_count
from {{ ref('stg_csds_newborn_hearing_screening') }}
where newborn_hearing_audiology_outcome is not null

{% for condition in [
    'phenylketonuria', 'sickle_cell_disease', 'cystic_fibrosis', 'congenital_hypothyroidism',
    'medium_chain_acyl_coa_dehydrogenase_deficiency', 'homocystinuria', 'maple_syrup_urine_disease',
    'glutaric_aciduria_type_1', 'isovaleric_aciduria'
] %}
union all

select
    source_record_id, cyp604_unique_id::varchar, 'CYP604'
    , 'newborn_blood_spot_{{ condition }}', person_id, organisation_code_provider
    , local_patient_identifier_extended, null::varchar, null::varchar
    , blood_spot_card_completion_date, 'blood_spot_card_completion'
    , 'code_list', null::varchar
    , 'newborn_blood_spot_test_outcome_status', outcome_{{ condition }}
    , unique_submission_id, reporting_period_start_date, reporting_period_end_date, effective_from, csds_version, file_type
    , first_reported_period_end_date, last_reported_period_end_date, accepted_source_record_count
from {{ ref('stg_csds_blood_spot_result') }}
where outcome_{{ condition }} is not null
{% endfor %}

{% for area in ['hips', 'heart', 'eyes', 'testes'] %}
union all

select
    source_record_id, cyp605_unique_id::varchar, 'CYP605'
    , 'infant_physical_examination_{{ area }}', person_id, organisation_code_provider
    , local_patient_identifier_extended, null::varchar, null::varchar
    , infant_physical_examination_date, 'infant_physical_examination'
    , 'code_list', null::varchar
    , 'infant_physical_examination_result', result_{{ area }}
    , unique_submission_id, reporting_period_start_date, reporting_period_end_date, effective_from, csds_version, file_type
    , first_reported_period_end_date, last_reported_period_end_date, accepted_source_record_count
from {{ ref('stg_csds_infant_physical_examination') }}
where result_{{ area }} is not null
{% endfor %}
)

-- Activity-linked sections keep each accepted occurrence, like CYP612. CYP611 units are fixed by ETOS.
, activity_items as (
select
    'CYP610' as source_table, cyp610_unique_id::varchar as source_row_id, 'breastfeeding_status' as clinical_record_type
    , 'code_list' as coding_scheme_kind, 'breastfeeding_status' as code_set_name
    , breastfeeding_status as clinical_code, null::varchar as clinical_value, null::varchar as unit_of_measurement_code
    , person_id, organisation_code_provider, unique_care_activity_identifier, dmic_observation_date
    , unique_submission_id, reporting_period_start_date, reporting_period_end_date, effective_from, csds_version, file_type
from {{ ref('stg_csds_breastfeeding_status') }}
{% for record_type, column_name, unit in [
    ('person_weight', 'person_weight', 'kg'),
    ('person_height', 'person_height_in_metres', 'm'),
    ('person_length', 'person_length_in_centimetres', 'cm')
] %}
union all
select
    'CYP611', cyp611_unique_id::varchar, '{{ record_type }}'
    , 'data_element', null::varchar
    , null::varchar, rtrim(rtrim({{ column_name }}::varchar, '0'), '.'), '{{ unit }}'
    , person_id, organisation_code_provider, unique_care_activity_identifier, dmic_observation_date
    , unique_submission_id, reporting_period_start_date, reporting_period_end_date, effective_from, csds_version, file_type
from {{ ref('stg_csds_observation') }}
where {{ column_name }} is not null
{% endfor %}
)

select
    e.*
    , null::varchar as code_set_name
from existing_records as e

union all

select
    i.source_record_id as originating_source_record_id
    , i.source_row_id
    , i.source_table
    , i.clinical_record_type
    , i.person_id
    , i.organisation_code_provider as provider_organisation_code
    , i.local_patient_id
    , i.immunisation_responsible_organisation_code
    , i.referral_source_record_id
    , null::varchar as care_activity_source_record_id
    , null::varchar as unique_care_activity_identifier
    , null::varchar as unique_care_contact_identifier
    , i.clinical_date::timestamp_ntz as clinical_at
    , iff(i.clinical_date is not null, 'date', null) as clinical_time_precision
    , iff(i.clinical_date is not null, i.clinical_time_basis, 'not_recorded') as clinical_time_basis
    , i.clinical_date as source_clinical_date
    , null::date as source_derived_date
    , i.coding_scheme_kind
    , i.coding_scheme_code
    , i.clinical_code
    , null::varchar as clinical_value
    , null::varchar as unit_of_measurement_code
    , i.unique_submission_id
    , i.reporting_period_start_date
    , i.reporting_period_end_date
    , i.effective_from as source_file_received_at
    , i.csds_version
    , i.file_type
    , i.first_reported_period_end_date
    , i.last_reported_period_end_date
    , i.accepted_source_record_count
    , null::boolean as is_care_activity_linked
    , null::boolean as is_care_activity_person_consistent
    , null::boolean as is_submitted_contact_person_consistent
    , i.code_set_name
from submitted_items as i

union all

{%- set can_inherit -%}
    a.source_record_id is not null and a.care_contact_at is not null and a.can_inherit_contact_time
        and (r.person_id is null or a.person_id is null or r.person_id = a.person_id)
{%- endset %}
select
    {{ dbt_utils.generate_surrogate_key(['r.source_table', 'r.unique_submission_id', 'r.source_row_id']) }} as originating_source_record_id
    , r.source_row_id
    , r.source_table
    , r.clinical_record_type
    , r.person_id
    , r.organisation_code_provider as provider_organisation_code
    , null::varchar as local_patient_id
    , null::varchar as immunisation_responsible_organisation_code
    , a.referral_id as referral_source_record_id
    , {{ dbt_utils.generate_surrogate_key(['r.unique_submission_id', 'r.unique_care_activity_identifier']) }} as care_activity_source_record_id
    , r.unique_care_activity_identifier
    , a.contact_id as unique_care_contact_identifier
    , iff({{ can_inherit }}, a.care_contact_at, null) as clinical_at
    , iff({{ can_inherit }}, a.care_contact_time_precision, null) as clinical_time_precision
    , iff({{ can_inherit }}, 'same_submission_care_activity_contact', 'not_recorded') as clinical_time_basis
    , null::date as source_clinical_date
    , r.dmic_observation_date as source_derived_date
    , r.coding_scheme_kind
    , null::varchar as coding_scheme_code
    , r.clinical_code
    , r.clinical_value
    , r.unit_of_measurement_code
    , r.unique_submission_id
    , r.reporting_period_start_date
    , r.reporting_period_end_date
    , r.effective_from as source_file_received_at
    , r.csds_version
    , r.file_type
    , r.reporting_period_end_date::date as first_reported_period_end_date
    , r.reporting_period_end_date::date as last_reported_period_end_date
    , 1::number as accepted_source_record_count
    , a.source_record_id is not null as is_care_activity_linked
    , iff(a.source_record_id is null or r.person_id is null or a.person_id is null, null, r.person_id = a.person_id) as is_care_activity_person_consistent
    , a.is_submitted_contact_person_consistent
    , r.code_set_name
from activity_items as r
left join activities as a
    on r.unique_submission_id = a.submission_id
    and r.unique_care_activity_identifier = a.activity_id
    and r.organisation_code_provider = a.provider_organisation_code
