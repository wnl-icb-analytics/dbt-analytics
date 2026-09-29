with expected as (
    select
        person_id,
        organisation_code_provider,
        procedure_scheme_in_use_community_care,
        immunisation_procedure_clinical_terminology,
        immunisation_date::date as immunisation_date,
        iff(person_id is null or organisation_code_provider is null or procedure_scheme_in_use_community_care is null or immunisation_procedure_clinical_terminology is null or immunisation_date::date is null, cyp501_unique_id, null) as incomplete_key_source_row_id,
        count(*) as accepted_source_record_count
    from {{ ref('stg_csds_coded_immunisation') }}
    group by all
)
, actual as (
    select
        person_id,
        organisation_code_provider,
        procedure_scheme_in_use_community_care,
        immunisation_procedure_clinical_terminology,
        immunisation_date::date as immunisation_date,
        iff(person_id is null or organisation_code_provider is null or procedure_scheme_in_use_community_care is null or immunisation_procedure_clinical_terminology is null or immunisation_date::date is null, cyp501_unique_id, null) as incomplete_key_source_row_id,
        source_record_id, cyp501_unique_id, accepted_source_record_count
    from {{ ref('int_csds_coded_immunisation') }}
)
select a.source_record_id, a.cyp501_unique_id,
    e.person_id, e.organisation_code_provider,
    e.procedure_scheme_in_use_community_care,
    e.immunisation_procedure_clinical_terminology, e.immunisation_date,
    e.incomplete_key_source_row_id,
    e.accepted_source_record_count as expected_source_count,
    a.accepted_source_record_count as represented_source_count
from expected as e
full outer join actual as a on e.person_id is not distinct from a.person_id
    and e.organisation_code_provider is not distinct from a.organisation_code_provider
    and e.procedure_scheme_in_use_community_care is not distinct from a.procedure_scheme_in_use_community_care
    and e.immunisation_procedure_clinical_terminology is not distinct from a.immunisation_procedure_clinical_terminology
    and e.immunisation_date is not distinct from a.immunisation_date
    and e.incomplete_key_source_row_id is not distinct from a.incomplete_key_source_row_id
where e.accepted_source_record_count is distinct from a.accepted_source_record_count
