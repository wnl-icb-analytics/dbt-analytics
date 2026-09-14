{{ config(materialized = 'table') }}

/*
Alcohol or drug involvement in injuries presenting to A&E (ECDS).

One row per involvement record. An attendance can carry more than one, for
example alcohol and a drug; nothing is selected or discarded here.
*/

select
    {{ dbt_utils.generate_surrogate_key(['i.primarykey_id', 'i.alcohol_drug_involvements_id']) }} as involvement_id
    , i.primarykey_id as visit_occurrence_id
    , sa.sk_patient_id
    , sa.start_date as attendance_date
    , sa.organisation_id
    , sa.site_id
    , sa.department_type
    , i.alcohol_drug_involvements_id as involvement_sequence
    , i.code as involvement_code
    , d.snomed_uk_preferred_term as involvement_desc
    , d.ecds_group1 as involvement_ecds_group1
from {{ ref('stg_sus_ecds_clinical_injury_alcohol_drug_involvements') }} as i
left join {{ ref('int_sus_uec_encounter') }} as sa
    on i.primarykey_id = sa.visit_occurrence_id
left join {{ ref('stg_dictionary_ecds_injurydrugalcohol') }} as d
    on i.code = d.snomed_code
