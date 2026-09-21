select
    person_id
    , 'referral' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_referral') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'contact' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_care_contact') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'activity' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_care_activity') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'clinical_item' as evidence_type
    , count(*) as n_records
    , min(first_reported_period_end_date) as first_reporting_period_end_date
    , max(last_reported_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_clinical_record') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'hospital_spell' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_hospital_provider_spell') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'ward_stay' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_ward_stay') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'legal_status' as evidence_type
    , count(*) as n_records
    , min(last_submission_period_end_date) as first_reporting_period_end_date
    , max(last_submission_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_mental_health_act_period') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'patient_indicator' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('stg_mhsds_patientindicators') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'patient_snapshot' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('stg_mhsds_mpi_history') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'activity_staff_link' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('rel_mhsds_care_activity_staff') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'referral_team_link' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('stg_mhsds_referral_service_team_history') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'indirect_activity' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_indirect_activity') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'referral_submission' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('stg_mhsds_referral_history') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'contact_submission' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('stg_mhsds_carecontact') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'accommodation_observation' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_accommodation_observation') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'employment_observation' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_employment_observation') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'disability_observation' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_disability_observation') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'social_circumstance_observation' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_social_circumstance_observation') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'care_plan_period' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_care_plan_period') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'care_plan_agreement' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_care_plan_agreement') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'referral_to_treatment_period' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_referral_to_treatment_period') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'community_treatment_order' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_community_treatment_order') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'community_treatment_order_recall' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_community_treatment_order_recall') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'restrictive_intervention_incident' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_restrictive_intervention_incident') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'restrictive_intervention_type' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_restrictive_intervention_type') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'home_leave' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_home_leave') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'leave_of_absence' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_leave_of_absence') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'absence_without_leave' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_absence_without_leave') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'spell_commissioner_period' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_spell_commissioner_period') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'discharge_readiness_period' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('fct_mhsds_discharge_readiness_period') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'clustering_assessment_header' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('stg_mhsds_clustering_assessment') }}
where person_id is not null
group by person_id

union all

select
    person_id
    , 'clustering_assessment_response_submission' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('stg_mhsds_clustering_assessment_response') }}
where person_id is not null
group by person_id

union all

select person_id
    , 'community_treatment_order_submission' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('stg_mhsds_community_treatment_order') }}
where person_id is not null
group by person_id

union all

select person_id
    , 'community_treatment_order_recall_submission' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('stg_mhsds_community_treatment_order_recall') }}
where person_id is not null
group by person_id

union all

select person_id
    , 'restrictive_intervention_incident_submission' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('stg_mhsds_restrictive_intervention_incident') }}
where person_id is not null
group by person_id

union all

select person_id
    , 'restrictive_intervention_type_submission' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('stg_mhsds_restrictive_intervention_type') }}
where person_id is not null
group by person_id

union all

select person_id
    , 'home_leave_submission' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('stg_mhsds_home_leave') }}
where person_id is not null
group by person_id

union all

select person_id
    , 'leave_of_absence_submission' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('stg_mhsds_leave_of_absence') }}
where person_id is not null
group by person_id

union all

select person_id
    , 'absence_without_leave_submission' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('stg_mhsds_absence_without_leave') }}
where person_id is not null
group by person_id

union all

select person_id
    , 'spell_commissioner_period_submission' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('stg_mhsds_spell_commissioner_period') }}
where person_id is not null
group by person_id

union all

select person_id
    , 'discharge_readiness_submission' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('stg_mhsds_discharge_readiness') }}
where person_id is not null
group by person_id

union all

select person_id
    , 'ward_stay_history_submission' as evidence_type
    , count(*) as n_records
    , min(reporting_period_end_date) as first_reporting_period_end_date
    , max(reporting_period_end_date) as last_reporting_period_end_date
from {{ ref('stg_mhsds_ward_stay_history') }}
where person_id is not null
group by person_id
