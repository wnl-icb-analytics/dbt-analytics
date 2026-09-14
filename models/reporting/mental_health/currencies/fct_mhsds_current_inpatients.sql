/*
People currently occupying a mental health bed.

A spell counts as current when int_mhsds_spell_encounters classifies it as
'open': no discharge date, and the spell appears in an active submission
within 2 reporting periods of the latest — the strongest available evidence
of continuing occupancy. Orphaned undischarged spells (stopped being
submitted) are excluded.

Upstream occupancy rules retain at most one current spell per person.
*/

select
    c.uniq_hosp_prov_spell_num
    , c.uniq_serv_req_id
    , c.person_id
    , c.sk_patient_id
    , c.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , c.dm_icb_commissioner as source_derived_icb_commissioner_code
    , source_derived_icb.organisation_name as source_derived_icb_commissioner_name
    , c.commissioner_icb_code
    , currency_commissioner.organisation_name as commissioner_icb_name
    , coalesce(currency_commissioner.is_wnl_commissioner, false) as is_wnl_commissioner
    , c.start_date_hosp_prov_spell as hospital_provider_spell_start_date
    , datediff(day, c.start_date_hosp_prov_spell, current_date) as days_in_bed
    , datediff(month, c.start_date_hosp_prov_spell, current_date) as months_in_bed
    , c.last_submission_period_end as last_submission_period_end_date
    , c.age_hosp_start_date as age_at_admission
    , c.is_cyp
    , c.has_known_age_at_admission
    , c.mh_admitted_patient_class as mental_health_admitted_patient_classification_type_code
    , patient_class.description as mental_health_admitted_patient_classification_type_description
    , c.setting_code
    , c.setting_name
    , c.icd10_3
    , icd.description as icd10_3_description
    , c.diagnosis_category
    , c.winning_tier
    , c.currency_group
    , c.currency_code
from {{ ref('int_mhsds_spell_currency') }} as c
left join {{ ref('int_mhsds_organisation') }} as provider
    on c.org_id_prov = provider.organisation_code
left join {{ ref('int_mhsds_organisation') }} as source_derived_icb
    on c.dm_icb_commissioner = source_derived_icb.organisation_code
left join {{ ref('int_mhsds_organisation') }} as currency_commissioner
    on c.commissioner_icb_code = currency_commissioner.organisation_code
left join {{ ref('mental_health_admitted_patient_classification_type') }} as patient_class
    on upper(trim(c.mh_admitted_patient_class)) = patient_class.code
left join {{ ref('stg_dictionary_dbo_diagnosis') }} as icd
    on upper(trim(c.icd10_3)) = icd.code
where c.end_date_source = 'open'
