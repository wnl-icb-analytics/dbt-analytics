select 'provider_label_mismatch' as failure_reason, f.source_record_id, f.clinical_record_type
from {{ ref('fct_csds_clinical_record') }} as f
inner join {{ ref('organisation') }} as o on upper(trim(f.provider_organisation_code)) = o.organisation_code
where f.provider_organisation_name is distinct from o.organisation_name

union all

select 'responsible_organisation_label_mismatch', f.source_record_id, f.clinical_record_type
from {{ ref('fct_csds_clinical_record') }} as f
inner join {{ ref('organisation') }} as o
    on upper(trim(f.immunisation_responsible_organisation_code)) = o.organisation_code
where f.immunisation_responsible_organisation_name is distinct from o.organisation_name

union all

select 'nonpositive_dictionary_mapping', source_record_id, clinical_record_type
from {{ ref('fct_csds_clinical_record') }}
where dictionary_snomed_code is not null and try_to_number(dictionary_snomed_code) <= 0

union all

select 'rounded_assessment_published_as_score', source_record_id, clinical_record_type
from {{ ref('fct_csds_clinical_record') }}
where assessment_score_numeric is not null and clinical_value_parse_status <> 'numeric'
