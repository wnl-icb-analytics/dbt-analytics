{{ config(materialized="table") }}
with investigation_codes as (
    select primarykey_id
        ,code
        ,snomed_id
        ,count(*) as observation_count
    from {{ref('stg_sus_ecds_clinical_investigations_snomed')}}
    where code is not null
    group by primarykey_id, code, snomed_id
),
investigations as (
    select primarykey_id
        ,code
        ,sum(observation_count) as observation_count
        ,array_agg(snomed_id) within group (order by snomed_id asc) as ordered_id_array
        ,'investigation' as observation_type
    from investigation_codes
    group by primarykey_id, code
),
inv_dict as(
    select snomed_code, 
        snomed_uk_preferred_term, 
        ecds_description, 
        ecds_group1,
        from {{ref('stg_dictionary_ecds_investigation')}}
),
treatment_codes as (
    select primarykey_id
        ,code
        ,snomed_id
        ,count(*) as observation_count
    from {{ref('stg_sus_ecds_clinical_treatments_snomed')}}
    where code is not null
    group by primarykey_id, code, snomed_id
),
treatments as (
    select primarykey_id
        ,code
        ,sum(observation_count) as observation_count
        ,array_agg(snomed_id) within group (order by snomed_id asc) as ordered_id_array
        ,'treatment' as observation_type
    from treatment_codes
    group by primarykey_id, code
),
treat_dict as(
    select snomed_code, 
        snomed_uk_preferred_term, 
        ecds_description, 
        ecds_group1,
        from {{ref('stg_dictionary_ecds_treatment')}}
),
comorb_codes as (
    select primarykey_id
        ,code
        ,comorbidities_id
        ,count(*) as observation_count
    from {{ref('stg_sus_ecds_clinical_comorbidities')}}
    where code is not null
    group by primarykey_id, code, comorbidities_id
),
comorbs as (
    select primarykey_id
        ,code
        ,sum(observation_count) as observation_count
        ,array_agg(comorbidities_id) within group (order by comorbidities_id asc) as ordered_id_array
        ,'comorbs' as observation_type
    from comorb_codes
    group by primarykey_id, code
),
comorb_dict as(
    select snomed_code, 
        snomed_uk_preferred_term, 
        ecds_description, 
        ecds_group1,
        -- null as cds_investigation_mapping_that_is_used_for_hrg_grouping,
        from {{ref('stg_dictionary_ecds_comorbidity')}}
),
finding_codes as (
    select primarykey_id
        ,code
        ,coded_findings_id
        ,count(*) as observation_count
    from {{ref('stg_sus_ecds_clinical_coded_findings')}}
    where code is not null
    group by primarykey_id, code, coded_findings_id
),
findings as (
    select primarykey_id
        ,code
        ,sum(observation_count) as observation_count
        ,array_agg(coded_findings_id) within group (order by coded_findings_id asc) as ordered_id_array
        ,'findings' as observation_type
    from finding_codes
    group by primarykey_id, code
), 
find_dict as(
    select snomed_code, 
        snomed_uk_preferred_term, 
        ecds_description, 
        ecds_group1,
        -- null as cds_investigation_mapping_that_is_used_for_hrg_grouping,
        from {{ref('stg_dictionary_ecds_codedfinding')}}
),
all_obs as(
    select *
    from investigations
    left join inv_dict on inv_dict.snomed_code = investigations.code
    union all
    select *
    from treatments
    left join treat_dict on treat_dict.snomed_code = treatments.code
    union all
    select *
    from comorbs
    left join comorb_dict on comorb_dict.snomed_code = comorbs.code
    union all
    select *
    from findings
    left join find_dict on find_dict.snomed_code = findings.code
)

select 
 {{ dbt_utils.generate_surrogate_key(["f.primarykey_id", "f.observation_type", "f.code"]) }} as event_id,
    sa.sk_patient_id,
    f.primarykey_id as visit_occurrence_id,
    f.observation_type,
    sa.start_date as date,
    sa.visit_occurrence_type,
    sa.organisation_id,
    sa.organisation_name,  
    sa.site_id,
    sa.site_name,
    sa.department_type,
    f.observation_count,
    f.ordered_id_array,
    f.code as snomed_code,
    f.ecds_description,
    f.ecds_group1,
    f.snomed_uk_preferred_term as snomed_decription

from all_obs as f

/* Diagnosis code for infering reason */
left join {{ref('int_sus_uec_encounter')}}  as sa on sa.visit_occurrence_id = f.primarykey_id
