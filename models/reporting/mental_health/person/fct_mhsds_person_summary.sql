with population as (
    select
        person_id
        , min(first_reporting_period_end_date) as first_evidence_reporting_period_end_date
        , max(last_reporting_period_end_date) as last_evidence_reporting_period_end_date
        , count(*) as n_evidence_types
        , sum(iff(evidence_type = 'indirect_activity', n_records, 0)) as n_indirect_activity_occurrences
        , sum(iff(evidence_type = 'care_plan_period', n_records, 0)) as n_care_plan_snapshots
        , sum(iff(evidence_type = 'community_treatment_order', n_records, 0)) as n_recorded_community_treatment_orders
    from {{ ref('int_mhsds_person_evidence') }}
    group by person_id
)
, caseload as (
    select person_id
        , count(*) as n_current_caseload_providers
        , sum(n_open_referrals) as n_current_caseload_referrals
        , sum(n_open_referrals_without_attendance) as n_current_caseload_referrals_without_attendance
        , max(evidence_date) as current_caseload_evidence_date
    from {{ ref('fct_mhsds_current_caseload_person') }}
    group by person_id
)
, referrals as (
    select person_id
        , count(*) as n_recorded_referrals
        , count_if(referral_status = 'open') as n_referrals_without_recorded_end
        , min(referral_received_date) as first_referral_date
        , max(referral_received_date) as latest_referral_date
    from {{ ref('fct_mhsds_referral') }}
    where person_id is not null
    group by person_id
)
, contacts as (
    select c.person_id
        , count(*) as n_recorded_contacts
        , max(iff(c.care_contact_date <= d.as_of_date, c.care_contact_date, null)) as latest_contact_date
        , max(iff(c.care_contact_date <= d.as_of_date and c.is_attended,
            c.care_contact_date, null)) as latest_attended_contact_date
        , count_if(c.care_contact_date between dateadd(month, -12, d.as_of_date) and d.as_of_date)
            as n_contacts_12m
        , count_if(c.care_contact_date between dateadd(day, -90, d.as_of_date) and d.as_of_date)
            as n_contacts_90d
        , count_if(c.is_attended
            and c.care_contact_date between dateadd(month, -12, d.as_of_date) and d.as_of_date)
            as n_attended_contacts_12m
        , count_if(c.is_dna
            and c.care_contact_date between dateadd(month, -12, d.as_of_date) and d.as_of_date)
            as n_dna_contacts_12m
        , count_if(c.is_cancelled
            and c.care_contact_date between dateadd(month, -12, d.as_of_date) and d.as_of_date)
            as n_cancelled_contacts_12m
        , count_if(c.attendance_status_code is null
            and c.care_contact_date between dateadd(month, -12, d.as_of_date) and d.as_of_date)
            as n_contacts_with_missing_attendance_status_12m
        , count_if(c.care_contact_date > d.as_of_date) as n_contacts_after_as_of_date
    from {{ ref('fct_mhsds_care_contact') }} as c
    cross join {{ ref('int_mhsds_reporting_date') }} as d
    where c.person_id is not null
    group by c.person_id
)
, recorded_spells as (
    select person_id
        , count(*) as n_recorded_hospital_spells
        , max(admission_date) as latest_recorded_admission_date
        , max(discharge_date) as latest_recorded_discharge_date
    from {{ ref('fct_mhsds_hospital_provider_spell') }}
    where person_id is not null
    group by person_id
)
, occupancy as (
    select person_id
        , count(*) as n_occupancy_intervals
        , count_if(is_current_inpatient) > 0 as is_current_inpatient
        , max(admission_date) as latest_occupancy_admission_date
        , max(iff(occupancy_end_reason = 'discharged', occupancy_end_date, null))
            as latest_occupancy_discharge_date
        , max(occupancy_evidence_as_of_date) as occupancy_evidence_as_of_date
        , max(last_submission_period_end_date) as last_inpatient_evidence_date
    from {{ ref('fct_mhsds_inpatient_occupancy') }}
    where person_id is not null
    group by person_id
)
, diagnosis_dates as (
    select person_id
        , max(iff(diagnosis_role = 'primary_diagnosis', diagnosis_recorded_at, null))
            as latest_primary_diagnosis_recorded_at
    from {{ ref('fct_mhsds_diagnosis') }}
    where person_id is not null
    group by person_id
)
, diagnoses as (
    select d.person_id
        , count(*) as n_diagnosis_records
        , count_if(d.diagnosis_role = 'primary_diagnosis') as n_primary_diagnosis_records
        , count_if(d.diagnosis_role = 'primary_diagnosis' and d.diagnosis_recorded_at is null)
            as n_undated_primary_diagnosis_records
        , count_if(d.diagnosis_role = 'primary_diagnosis'
            and d.diagnosis_recorded_at = t.latest_primary_diagnosis_recorded_at)
            as n_latest_primary_diagnosis_records
        , max(t.latest_primary_diagnosis_recorded_at) as latest_primary_diagnosis_recorded_at
    from {{ ref('fct_mhsds_diagnosis') }} as d
    left join diagnosis_dates as t on d.person_id = t.person_id
    where d.person_id is not null
    group by d.person_id
)
, assessments as (
    select person_id
        , count(*) as n_assessment_observations
        , max(assessment_recorded_at) as latest_assessment_recorded_at
    from {{ ref('fct_mhsds_assessment_observation') }}
    where person_id is not null
    group by person_id
)
, legal_status as (
    select person_id
        , count(*) as n_legal_status_periods
        , count_if(is_detention_period) > 0 as has_recorded_detention
        , count_if(is_current_detention_period) > 0 as is_currently_detained
    from {{ ref('fct_mhsds_mental_health_act_period') }}
    where person_id is not null
    group by person_id
)
, latest_detention as (
    select person_id
        , legal_status_start_date as latest_detention_start_date
        , legal_status_code as latest_detention_code
        , legal_status_description as latest_detention_description
    from {{ ref('fct_mhsds_mental_health_act_period') }}
    where is_detention_period and person_id is not null
    qualify row_number() over (
        partition by person_id
        order by legal_status_start_date desc nulls last,
            last_submission_period_end_date desc nulls last, mental_health_act_period_id desc
    ) = 1
)
, patient_indicators as (
    select person_id
        , upper(cpp) as child_protection_plan_status_code
        , upper(lac_status) as looked_after_child_indicator_code
        , reporting_period_end_date as patient_indicator_reporting_period_end_date
    from {{ ref('stg_mhsds_patientindicators') }}
    where person_id is not null
    qualify row_number() over (
        partition by person_id
        order by reporting_period_end_date desc nulls last, effective_from desc nulls last,
            uniq_submission_id desc, mhs005_uniq_id desc
    ) = 1
)
select
    p.person_id
    , b.sk_patient_id
    , d.as_of_date
    , current_date as calculation_date
    , p.first_evidence_reporting_period_end_date
    , p.last_evidence_reporting_period_end_date
    , p.n_evidence_types
    , coalesce(r.n_recorded_referrals, 0) as n_recorded_referrals
    , coalesce(r.n_referrals_without_recorded_end, 0) as n_referrals_without_recorded_end
    , coalesce(cl.n_current_caseload_providers, 0) as n_current_caseload_providers
    , coalesce(cl.n_current_caseload_referrals, 0) as n_current_caseload_referrals
    , coalesce(cl.n_current_caseload_referrals_without_attendance, 0) as n_current_caseload_referrals_without_attendance
    , coalesce(cl.n_current_caseload_referrals > 0, false) as has_current_recorded_caseload
    , cl.current_caseload_evidence_date
    , p.n_indirect_activity_occurrences
    , p.n_care_plan_snapshots
    , p.n_recorded_community_treatment_orders
    , r.first_referral_date
    , r.latest_referral_date
    , coalesce(c.n_recorded_contacts, 0) as n_recorded_contacts
    , c.latest_contact_date
    , c.latest_attended_contact_date
    , coalesce(c.n_contacts_12m, 0) as n_contacts_12m
    , coalesce(c.n_contacts_90d, 0) as n_contacts_90d
    , coalesce(c.n_attended_contacts_12m, 0) as n_attended_contacts_12m
    , coalesce(c.n_dna_contacts_12m, 0) as n_dna_contacts_12m
    , coalesce(c.n_cancelled_contacts_12m, 0) as n_cancelled_contacts_12m
    , coalesce(c.n_contacts_with_missing_attendance_status_12m, 0)
        as n_contacts_with_missing_attendance_status_12m
    , coalesce(c.n_contacts_after_as_of_date, 0) as n_contacts_after_as_of_date
    , coalesce(s.n_recorded_hospital_spells, 0) as n_recorded_hospital_spells
    , s.latest_recorded_admission_date
    , s.latest_recorded_discharge_date
    , coalesce(o.n_occupancy_intervals, 0) as n_occupancy_intervals
    , coalesce(o.is_current_inpatient, false) as is_current_inpatient
    , o.latest_occupancy_admission_date
    , o.latest_occupancy_discharge_date
    , o.occupancy_evidence_as_of_date
    , o.last_inpatient_evidence_date
    , coalesce(dx.n_diagnosis_records, 0) as n_diagnosis_records
    , coalesce(dx.n_primary_diagnosis_records, 0) as n_primary_diagnosis_records
    , coalesce(dx.n_undated_primary_diagnosis_records, 0) as n_undated_primary_diagnosis_records
    , dx.latest_primary_diagnosis_recorded_at
    , coalesce(dx.n_latest_primary_diagnosis_records, 0) as n_latest_primary_diagnosis_records
    , coalesce(a.n_assessment_observations, 0) as n_assessment_observations
    , a.latest_assessment_recorded_at
    , coalesce(m.n_legal_status_periods, 0) as n_legal_status_periods
    , coalesce(m.has_recorded_detention, false) as has_recorded_detention
    , coalesce(m.is_currently_detained, false) as is_currently_detained
    , ld.latest_detention_start_date
    , ld.latest_detention_code
    , ld.latest_detention_description
    , i.child_protection_plan_status_code
    , cpp.description as child_protection_plan_status_description
    , i.looked_after_child_indicator_code
    , lac.description as looked_after_child_indicator_description
    , case i.looked_after_child_indicator_code when 'Y' then true when 'N' then false end
        as is_looked_after_child
    , i.patient_indicator_reporting_period_end_date
from population as p
cross join {{ ref('int_mhsds_reporting_date') }} as d
left join {{ ref('stg_mhsds_bridging') }} as b on p.person_id = b.person_id
left join caseload as cl on p.person_id = cl.person_id
left join referrals as r on p.person_id = r.person_id
left join contacts as c on p.person_id = c.person_id
left join recorded_spells as s on p.person_id = s.person_id
left join occupancy as o on p.person_id = o.person_id
left join diagnoses as dx on p.person_id = dx.person_id
left join assessments as a on p.person_id = a.person_id
left join legal_status as m on p.person_id = m.person_id
left join latest_detention as ld on p.person_id = ld.person_id
left join patient_indicators as i on p.person_id = i.person_id
left join {{ ref('mhsds_profile_code_lookup') }} as cpp
    on i.child_protection_plan_status_code = cpp.code
    and cpp.code_set_name = 'child_protection_plan_status'
left join {{ ref('mhsds_profile_code_lookup') }} as lac
    on i.looked_after_child_indicator_code = lac.code
    and lac.code_set_name = 'looked_after_child_indicator'
