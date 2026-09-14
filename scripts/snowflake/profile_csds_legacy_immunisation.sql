-- Read-only investigation of retained, unlabelled numeric Read-v2 immunisations.
-- Standalone Snowflake diagnostics, not a dbt model. Legacy views have no dbt interface.
-- Requires the established DEV clinical fact and references, plus legacy source access.
-- Every result contains only aggregate counts or reporting-period boundaries.
-- Same-person/provider/date matches are candidates for investigation, not code mappings.

select
    csds_version
    , count(*) as unlabelled_numeric_read_immunisations
    , count(distinct clinical_code) as distinct_tokens
    , min(reporting_period_start_date)::date as first_reporting_period
    , max(reporting_period_end_date)::date as last_reporting_period
from DEV__REPORTING.COMMUNITY.fct_csds_clinical_record
where clinical_record_type = 'immunisation'
    and clinical_code_system = 'Read v2'
    and clinical_description is null
    and regexp_like(clinical_code, '[0-9]+')
group by csds_version;

select
    count(*) as legacy_records
    , count(distinct "UniqueID_CYP501") as distinct_source_ids
    , count("ImmunisationProcedure") as populated_clinical_codes
    , count("MasterSnomedCTProcedureCode") as master_snomed_codes
    , count("MasterSnomedCTProcedureTerm") as master_snomed_terms
    , count("MapSnomedCTProcedureCode") as mapped_snomed_codes
from DATA_LAKE.SERVICES_DATA_NATIONAL."CSDS_cyp501codedimm_Deprecated";

select count(*) as legacy_bsp_records
from DATA_LAKE.SERVICES_DATA_NATIONAL."CSDSBSP_CYP501CodedImm_Deprecated";

with gaps as (
    select source_row_id, provider_organisation_code, person_id, clinical_date
    from DEV__REPORTING.COMMUNITY.fct_csds_clinical_record
    where clinical_record_type = 'immunisation'
        and clinical_code_system = 'Read v2'
        and clinical_description is null
        and regexp_like(clinical_code, '[0-9]+')
), legacy_ids as (
    select distinct "UniqueID_CYP501"::varchar as source_row_id, "OrgID_Provider" as provider
    from DATA_LAKE.SERVICES_DATA_NATIONAL."CSDS_cyp501codedimm_Deprecated"
), legacy_people as (
    select distinct "Person_ID" as person_id
    from DATA_LAKE.SERVICES_DATA_NATIONAL."CSDS_cyp501codedimm_Deprecated"
), legacy_days as (
    select
        "Person_ID" as person_id
        , "OrgID_Provider" as provider
        , "Immunisation_Date"::date as clinical_date
        , count(distinct "ImmunisationProcedure") as distinct_codes
    from DATA_LAKE.SERVICES_DATA_NATIONAL."CSDS_cyp501codedimm_Deprecated"
    group by 1, 2, 3
)
select
    count(*) as gap_records
    , count_if(i.source_row_id is not null) as same_source_id_and_provider
    , count_if(p.person_id is not null) as person_present_in_legacy
    , count_if(d.person_id is not null) as same_person_provider_date
    , count_if(d.distinct_codes = 1) as one_legacy_code_for_person_provider_date
from gaps as g
left join legacy_ids as i
    on g.source_row_id = i.source_row_id and g.provider_organisation_code = i.provider
left join legacy_people as p on g.person_id = p.person_id
left join legacy_days as d
    on g.person_id = d.person_id
    and g.provider_organisation_code = d.provider
    and g.clinical_date = d.clinical_date;

-- A single code on a legacy day still needs both terminology and occurrence evidence.
with gaps as (
    select provider_organisation_code, person_id, clinical_date
    from DEV__REPORTING.COMMUNITY.fct_csds_clinical_record
    where clinical_record_type = 'immunisation'
        and clinical_code_system = 'Read v2'
        and clinical_description is null
        and regexp_like(clinical_code, '[0-9]+')
), legacy_days as (
    select
        "Person_ID" as person_id
        , "OrgID_Provider" as provider
        , "Immunisation_Date"::date as clinical_date
        , count(distinct "ImmunisationProcedure") as distinct_codes
        , count(distinct "Procedure_Scheme") as distinct_schemes
        , min("ImmunisationProcedure") as code
        , min("Procedure_Scheme") as scheme
    from DATA_LAKE.SERVICES_DATA_NATIONAL."CSDS_cyp501codedimm_Deprecated"
    group by 1, 2, 3
), complete_read_terms as (
    select distinct "Read_Code"::varchar || "Term_Code"::varchar as code
    from UKHFD."Read_Data_Migration"."dim_RC_SCT_Map2_SCD"
)
select
    count(*) as single_code_and_scheme_candidates
    , count(distinct d.code) as distinct_candidate_tokens
    , count_if(trim(d.scheme) in ('04', '4')) as declared_read_v2_candidates
    , count_if(r.code is not null) as scheme_qualified_read_labels
    , count_if(t.code is not null and trim(d.scheme) in ('04', '4')) as complete_read_term_matches
from gaps as g
inner join legacy_days as d
    on g.person_id = d.person_id
    and g.provider_organisation_code = d.provider
    and g.clinical_date = d.clinical_date
    and d.distinct_codes = 1 and d.distinct_schemes = 1
left join DEV__REFERENCE.TERMINOLOGY.read_code as r
    on d.code = r.code
    and ((trim(d.scheme) in ('04', '4') and r.coding_system = 'read_v2')
        or (trim(d.scheme) in ('05', '5') and r.coding_system = 'ctv3'))
left join complete_read_terms as t on d.code = t.code;

-- Explicit text casts prevent implicit numeric conversion of cross-delivery identifiers.
with gaps as (
    select source_row_id, provider_organisation_code, person_id, clinical_date
    from DEV__REPORTING.COMMUNITY.fct_csds_clinical_record
    where clinical_record_type = 'immunisation'
        and clinical_code_system = 'Read v2'
        and clinical_description is null
        and regexp_like(clinical_code, '[0-9]+')
)
select
    count(*) as same_person_provider_date_join_rows
    , count_if(l."UniqueID_CYP501"::varchar = r."SK"::varchar) as same_legacy_id_and_source_sk
    , count_if(l."UniqueID_CYP501"::varchar = r."BSP UNIQUE ID"::varchar) as same_legacy_id_and_bsp_id
    , count_if(l."UniqueSubmissionID"::varchar = r."UNIQUE SUBMISSION ID"::varchar) as same_submission_id
    , count_if(l."LocalPatientID"::varchar = r."LOCAL PATIENT IDENTIFIER (EXTENDED)"::varchar) as same_local_patient_id
    , count_if(l."RecordNumber"::varchar = r."RECORD NUMBER"::varchar) as same_record_number
from gaps as g
inner join DATA_LAKE.CSDS."CYP501CodedImm" as r
    on g.source_row_id = r."CYP501 UNIQUE ID"::varchar
inner join DATA_LAKE.SERVICES_DATA_NATIONAL."CSDS_cyp501codedimm_Deprecated" as l
    on g.person_id = l."Person_ID"
    and g.provider_organisation_code = l."OrgID_Provider"
    and g.clinical_date = l."Immunisation_Date"::date;
