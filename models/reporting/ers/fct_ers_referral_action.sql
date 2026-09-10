{{ config(tags=['ers', 'large_periodic'], cluster_by=['sk_patient_id', 'action_at']) }}

select
    a.action_id as action_id,
    a.ubrn_id as ubrn_id,
    a.ubrn as ubrn,
    nullif(replace(trim(a.ubrn), '-', ''), '') as normalised_ubrn,
    {{ consistent_sk_patient_id_format('a.nhs_number_pseudo') }} as sk_patient_id,
    a.e_referral_pathway_start as pathway_started_at,
    a.action_dt_tm as action_at,
    {{ fin_year_from_date('a.action_dt_tm') }} as financial_year,
    {{ fin_month_from_date('a.action_dt_tm') }} as financial_month,
    dates.fiscal_calendar_month_name as financial_month_name,
    dates.end_of_iso_week_date as week_end_date,
    -- Keep context supplied with the action; current person details cannot replace it.
    a.patient_age as patient_age,
    a.patient_sex_cd::varchar as patient_sex_code,
    coalesce(sex.name, nullif(trim(a.patient_sex_desc), '')) as patient_sex_name,
    nullif(trim(a.patients_lsoa), '') as residence_lsoa_code,
    imd.imddecile as residence_imd_2019_decile,
    nullif(trim(a.patients_reg_gp_practice_id), '') as registered_practice_code,
    coalesce(practice.organisation_name, nullif(trim(a.patients_reg_gp_practice_name), ''))
        as registered_practice_name,
    nullif(trim(a.patients_la_of_residence_id), '') as residence_local_authority_code,
    coalesce(residence_la.name, nullif(trim(a.patients_la_of_residence_name), ''))
        as residence_local_authority_name,
    nullif(trim(a.patients_la_of_registration_id), '') as registration_local_authority_code,
    coalesce(registration_la.name, nullif(trim(a.patients_la_of_registration_name), ''))
        as registration_local_authority_name,
    nullif(trim(a.referrer_commissioner_id), '') as referrer_commissioner_code,
    coalesce(commissioner.organisation_name, nullif(trim(a.referrer_commissioner_name), ''))
        as referrer_commissioner_name,
    a.action_cd::varchar as action_code,
    coalesce(ac.name, nullif(trim(a.action_desc), '')) as action_name,
    a.action_reason_cd::varchar as action_reason_code,
    coalesce(ar.name, nullif(trim(a.action_reason_desc), '')) as action_reason_name,
    a.priority_cd::varchar as priority_code,
    coalesce(pr.name, nullif(trim(a.priority_desc), '')) as priority_name,
    a.specialty_cd::varchar as specialty_code,
    coalesce(sp.name, nullif(trim(a.specialty_desc), '')) as specialty_name,
    a.clinic_type_cd::varchar as clinic_type_code,
    coalesce(ct.name, nullif(trim(a.clinic_type_desc), '')) as clinic_type_name,
    a.referring_org_id as referring_organisation_code,
    coalesce(referrer.organisation_name, nullif(trim(a.referrer_org_name), '')) as referring_organisation_name,
    a.org_id as action_organisation_code,
    coalesce(actor.organisation_name, nullif(trim(a.org_name), '')) as action_organisation_name,
    a.service_id as service_id,
    coalesce(service.service_name, nullif(trim(a.service_name), '')) as service_name,
    a.service_specialty_cd::varchar as service_specialty_code,
    coalesce(ss.name, nullif(trim(a.service_specialty_desc), '')) as service_specialty_name,
    a.provider_org_id as provider_organisation_code,
    coalesce(provider.organisation_name, nullif(trim(a.provider_org_name), '')) as provider_organisation_name,
    a.location_org_id as site_code,
    coalesce(site.organisation_name, nullif(trim(a.location_org_name), '')) as site_name,
    a.appt_dt_tm as appointment_at,
    a.appt_type_cd::varchar as appointment_type_code,
    coalesce(atp.name, nullif(trim(a.appt_type_desc), '')) as appointment_type_name,
    a.rebooked_to_action_id as rebooked_to_action_id,
    a.initial_ubrn_id as initial_ubrn_id,
    a.previous_ubrn_id as previous_ubrn_id,
    a.initial_ubrn as initial_ubrn,
    a.previous_ubrn as previous_ubrn,
    a.next_ubrn as next_ubrn,
    a.clinical_assessment_outcome_cd::varchar as assessment_outcome_code,
    coalesce(ao.name, nullif(trim(a.clinical_assess_outcome_desc), '')) as assessment_outcome_name,
    a.ar_status_cd::varchar as advice_request_status_code,
    nullif(trim(a.ar_status_desc), '') as advice_request_status_name,
    a.uniq_submission_id as source_submission_id,
    a.dmic_date_added as source_imported_at
from {{ ref('stg_ers_ubrn_action') }} as a
left join {{ ref('ers_action') }} as ac
    on a.action_cd::varchar = ac.code
left join {{ ref('ers_action_reason') }} as ar
    on a.action_reason_cd::varchar = ar.code
left join {{ ref('ers_priority') }} as pr
    on a.priority_cd::varchar = pr.code
left join {{ ref('ers_specialty') }} as sp
    on a.specialty_cd::varchar = sp.code
left join {{ ref('ers_clinic_type') }} as ct
    on a.clinic_type_cd::varchar = ct.code
left join {{ ref('ers_specialty') }} as ss
    on a.service_specialty_cd::varchar = ss.code
left join {{ ref('ers_appointment_type') }} as atp
    on a.appt_type_cd::varchar = atp.code
left join {{ ref('ers_assessment_outcome') }} as ao
    on a.clinical_assessment_outcome_cd::varchar = ao.code
left join {{ ref('ers_organisation') }} as referrer
    on upper(trim(a.referring_org_id)) = referrer.organisation_code
left join {{ ref('ers_organisation') }} as actor
    on upper(trim(a.org_id)) = actor.organisation_code
left join {{ ref('ers_organisation') }} as provider
    on upper(trim(a.provider_org_id)) = provider.organisation_code
left join {{ ref('ers_organisation') }} as site
    on upper(trim(a.location_org_id)) = site.organisation_code
left join {{ ref('ers_service') }} as service
    on a.service_id = service.service_id
left join {{ ref('ers_patient_sex') }} as sex
    on a.patient_sex_cd::varchar = sex.code
left join {{ ref('ers_organisation') }} as practice
    on upper(trim(a.patients_reg_gp_practice_id)) = practice.organisation_code
left join {{ ref('ers_organisation') }} as commissioner
    on upper(trim(a.referrer_commissioner_id)) = commissioner.organisation_code
left join {{ ref('stg_reference_imd2019') }} as imd
    on a.patients_lsoa = imd.lsoacode
left join {{ ref('stg_dictionary_dbo_dates') }} as dates
    on a.action_dt_tm::date = dates.full_date
left join {{ ref('ons_geography') }} as residence_la
    on a.patients_la_of_residence_id = residence_la.code
left join {{ ref('ons_geography') }} as registration_la
    on a.patients_la_of_registration_id = registration_la.code
