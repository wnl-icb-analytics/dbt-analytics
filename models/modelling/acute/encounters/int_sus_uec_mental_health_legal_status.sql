{{ config(materialized = 'table') }}

/*
Mental Health Act legal status recorded during A&E attendances (ECDS).

One row per legal-status record. An attendance can carry more than one when
the status changes during the attendance (for example arriving under Section
136 and then being detained under Section 2); nothing is selected or
discarded here. Most records are 98 (not applicable) or 99 (not known).
*/

select
    {{ dbt_utils.generate_surrogate_key(['m.primarykey_id', 'm.mental_health_act_legal_status_id']) }} as legal_status_id
    , m.primarykey_id as visit_occurrence_id
    , sa.sk_patient_id
    , sa.start_date as attendance_date
    , sa.organisation_id
    , sa.site_id
    , sa.department_type
    , m.mental_health_act_legal_status_id as legal_status_sequence
    , m.legal_status_code
    , d.legal_status_desc
    , m.assigned_at
    , m.expires_at
from {{ ref('stg_sus_ecds_patient_mental_health_act_legal_status') }} as m
left join {{ ref('int_sus_uec_encounter') }} as sa
    on m.primarykey_id = sa.visit_occurrence_id
left join {{ ref('stg_ukhfd_mental_health_act_legal_status_classification') }} as d
    on m.legal_status_code = d.legal_status_code
