select
    {{ dbt_utils.generate_surrogate_key(['a.unique_submission_id', 'a.unique_care_activity_identifier']) }} as source_record_id
    , 'CSDS' as source_dataset
    , a.cyp202_unique_id as source_row_id
    , a.unique_care_activity_identifier as activity_id
    , a.care_activity_identifier as local_activity_id
    , a.unique_care_contact_identifier as contact_id
    , a.care_contact_identifier as local_contact_id
    , c.unique_service_request_identifier as referral_id
    , case when c.cyp201_unique_id is not null then
        {{ dbt_utils.generate_surrogate_key(['c.unique_service_request_identifier', 'c.unique_care_contact_identifier']) }}
    end as contact_source_record_id
    , c.cyp201_unique_id as contact_source_row_id
    , a.person_id
    , b.sk_patient_id
    , c.care_contact_date::date as care_contact_date
    , c.care_contact_time::time as care_contact_time
    , case
        when c.care_contact_date is null then null
        when c.care_contact_time is null then c.care_contact_date::date::timestamp_ntz
        else timestamp_ntz_from_parts(c.care_contact_date::date, c.care_contact_time::time)
    end as care_contact_at
    , case
        when c.care_contact_date is null then null
        when c.care_contact_time is null then 'date'
        else 'timestamp'
    end as care_contact_time_precision
    , c.ic_age_at_care_contact_date as age_at_contact
    , c.activity_location_type_code
    , loc.description as activity_location_type_name
    , a.community_care_activity_type as activity_type_code
    , activity_type.description as activity_type_name
    , a.clinical_contact_duration_of_care_activity as clinical_contact_duration_minutes
    , a.care_professional_local_identifier as local_lead_professional_id
    , a.unique_care_professional_id_local as lead_professional_id
    , a.procedure_scheme_in_use_community_care as procedure_scheme_code
    , procedure_scheme.description as procedure_scheme_name
    , a.coded_procedure_clinical_terminology as procedure_code
    , coalesce(procedure_snomed.preferred_term, procedure_read.term) as procedure_name
    , case when procedure_snomed.preferred_term is not null then 'Dictionary.SNOMED.Concept'
        when procedure_read.term is not null then procedure_read.definition_source
    end as procedure_name_definition_source
    , a.finding_scheme_in_use_community_care as finding_scheme_code
    , finding_scheme.description as finding_scheme_name
    , a.coded_finding_coded_clinical_entry as finding_code
    , coalesce(finding_snomed.preferred_term, finding_read.term, finding_icd.description) as finding_name
    , case when finding_snomed.preferred_term is not null then 'Dictionary.SNOMED.Concept'
        when finding_read.term is not null then finding_read.definition_source
        when finding_icd.description is not null then 'Dictionary.dbo.Diagnosis'
    end as finding_name_definition_source
    , a.observation_scheme_in_use_community_care as observation_scheme_code
    , observation_scheme.description as observation_scheme_name
    , a.coded_observation_clinical_terminology as observation_code
    , coalesce(observation_snomed.preferred_term, observation_read.term) as observation_name
    , case when observation_snomed.preferred_term is not null then 'Dictionary.SNOMED.Concept'
        when observation_read.term is not null then observation_read.definition_source
    end as observation_name_definition_source
    , a.observation_value
    , a.ucum_unit_of_measurement as observation_unit_code
    , coalesce(unit.description, unit_alias.description) as observation_unit_name
    , coalesce(unit.unit_symbol, unit_alias.unit_symbol) as observation_unit_symbol
    , coalesce(unit.definition_source, unit_alias.definition_source) as observation_unit_definition_source
    , coalesce(unit.match_type, iff(unit_alias.snomed_code is not null, 'csds_etos_alias', null)) as observation_unit_match_type
    , c.cyp201_unique_id is not null as is_submitted_contact_linked
    , case when c.cyp201_unique_id is null or a.person_id is null or c.person_id is null then null
        else a.person_id = c.person_id end as is_submitted_contact_person_consistent
    , a.organisation_code_provider as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , a.unique_submission_id as submission_id
    , a.reporting_period_start_date::date as reporting_period_start_date
    , a.reporting_period_end_date::date as reporting_period_end_date
    , a.effective_from as source_file_received_at
    , a.csds_version
    , a.file_type
    , latest_parent.source_record_id is not null as is_contact_linked
    , case when latest_parent.source_record_id is null or a.person_id is null or latest_parent.person_id is null then null
        else a.person_id = latest_parent.person_id end as is_contact_person_consistent
    , case when latest_parent.source_record_id is null or c.cyp201_unique_id is null then null
        else c.cyp201_unique_id = latest_parent.source_row_id end as is_contact_occurrence_consistent
from {{ ref('stg_csds_care_activity_history') }} as a
left join {{ ref('stg_csds_care_contact_history') }} as c
    on a.unique_submission_id = c.unique_submission_id
    and a.unique_care_contact_identifier = c.unique_care_contact_identifier
left join {{ ref('stg_csds_bridging') }} as b on a.person_id = b.person_id
left join {{ ref('activity_location_type') }} as loc on trim(c.activity_location_type_code) = loc.code
left join {{ ref('csds_activity_code_lookup') }} as activity_type
    on activity_type.code_set_name = 'activity_type' and upper(trim(a.community_care_activity_type)) = activity_type.code
left join {{ ref('csds_activity_code_lookup') }} as procedure_scheme
    on procedure_scheme.code_set_name = 'procedure_scheme' and upper(trim(a.procedure_scheme_in_use_community_care)) = procedure_scheme.code
left join {{ ref('csds_activity_code_lookup') }} as finding_scheme
    on finding_scheme.code_set_name = 'finding_scheme' and upper(trim(a.finding_scheme_in_use_community_care)) = finding_scheme.code
left join {{ ref('csds_activity_code_lookup') }} as observation_scheme
    on observation_scheme.code_set_name = 'observation_scheme' and upper(trim(a.observation_scheme_in_use_community_care)) = observation_scheme.code
left join {{ ref('fct_csds_care_contact') }} as latest_parent
    on latest_parent.referral_id = c.unique_service_request_identifier
    and latest_parent.contact_id = a.unique_care_contact_identifier
left join {{ ref('clinical_unit_of_measurement') }} as unit
    on trim(a.ucum_unit_of_measurement) = unit.code
left join {{ ref('organisation') }} as provider
    on upper(trim(a.organisation_code_provider)) = provider.organisation_code
left join {{ ref('stg_dictionary_snomed_concept') }} as procedure_snomed
    on trim(a.coded_procedure_clinical_terminology) = procedure_snomed.snomed_code
    and trim(a.procedure_scheme_in_use_community_care) = '06'
left join {{ ref('read_code') }} as procedure_read
    on trim(a.coded_procedure_clinical_terminology) = procedure_read.code
    and ((trim(a.procedure_scheme_in_use_community_care) = '04' and procedure_read.coding_system = 'read_v2')
        or (trim(a.procedure_scheme_in_use_community_care) = '05' and procedure_read.coding_system = 'ctv3'))
left join {{ ref('stg_dictionary_snomed_concept') }} as finding_snomed
    on trim(a.coded_finding_coded_clinical_entry) = finding_snomed.snomed_code
    and trim(a.finding_scheme_in_use_community_care) = '04'
left join {{ ref('read_code') }} as finding_read
    on trim(a.coded_finding_coded_clinical_entry) = finding_read.code
    and ((trim(a.finding_scheme_in_use_community_care) = '02' and finding_read.coding_system = 'read_v2')
        or (trim(a.finding_scheme_in_use_community_care) = '03' and finding_read.coding_system = 'ctv3'))
left join {{ ref('stg_dictionary_snomed_concept') }} as observation_snomed
    on trim(a.coded_observation_clinical_terminology) = observation_snomed.snomed_code
    and trim(a.observation_scheme_in_use_community_care) = '03'
left join {{ ref('read_code') }} as observation_read
    on trim(a.coded_observation_clinical_terminology) = observation_read.code
    and ((trim(a.observation_scheme_in_use_community_care) = '01' and observation_read.coding_system = 'read_v2')
        or (trim(a.observation_scheme_in_use_community_care) = '02' and observation_read.coding_system = 'ctv3'))
left join {{ ref('stg_dictionary_dbo_diagnosis') }} as finding_icd
    on {{ clean_icd10_code('upper(trim(a.coded_finding_coded_clinical_entry))') }} = upper(finding_icd.code)
    and trim(a.finding_scheme_in_use_community_care) = '01'
left join {{ ref('csds_observation_unit_alias') }} as unit_alias
    on unit.code is null
    and upper(trim(a.ucum_unit_of_measurement)) = unit_alias.source_unit_upper
    and case when trim(a.observation_scheme_in_use_community_care) = '03'
        then trim(a.coded_observation_clinical_terminology)
        else observation_read.snomed_ct_code end = unit_alias.snomed_code
