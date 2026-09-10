# e-RS field mapping

Selected expressions below come from the compiled models. Model YAML supplies the meanings.
See [analyst validation](ers-analyst-validation.md#specification-evidence) for the limits of public specification access.
An expression is traceable warehouse lineage, not evidence that the full current NHS specification was checked.

In the action fact, `a` is active submitted staging; other aliases are joined reference models.
In the referral and appointment facts, `h` is grouped action history and `a` is the selected latest action.
`s` is the selected service action and `status` the selected appointment action.
In the link models, `p` is appointment, `e` is referral and `a` is action or the explicitly named acute-source union.

## fct_ers_referral_action

| Column | Selected expression | Meaning |
|---|---|---|
| `action_id` | `a.action_id as action_id` | Sequential identifier supplied by e-RS. One row per active submitted action; orders the action history. |
| `ubrn_id` | `a.ubrn_id as ubrn_id` | Source identifier of the referral, triage or advice request. |
| `ubrn` | `a.ubrn as ubrn` | Supplied booking reference, preserved unchanged. |
| `normalised_ubrn` | `nullif(replace(trim(a.ubrn), '-', ''), '') as normalised_ubrn` | Booking reference with display hyphens and surrounding spaces removed. Use for recorded cross-source matching and also check patient-key agreement. |
| `sk_patient_id` | `nullif(nullif(trim(cast(a.nhs_number_pseudo as varchar)), ''), '1') as sk_patient_id` | Shared pseudonymised NHS-number key. Null for blank values and the source unknown marker 1; no demographic or registration filter is applied. |
| `pathway_started_at` | `a.e_referral_pathway_start as pathway_started_at` | Recorded e-RS referral-to-treatment pathway start. Not necessarily the first action time. |
| `action_at` | `a.action_dt_tm as action_at` | Recorded date and time of the action. |
| `financial_year` | `case when a.action_dt_tm is not null then to_varchar(iff(month(a.action_dt_tm) >= 4, year(a.action_dt_tm), year(a.action_dt_tm) - 1)) \|\| lpad(to_varchar(mod(iff(month(a.action_dt_tm) >= 4, year(a.action_dt_tm), year(a.action_dt_tm) - 1) + 1, 100)), 2, '0') end as financial_year` | NHS April-March financial year of action_at, formatted YYYYYY, for example 202526. Uses the shared financial-period macro. |
| `financial_month` | `mod(month(a.action_dt_tm) + 8, 12) + 1 as financial_month` | NHS financial month of action_at, where April is 1 and March is 12. |
| `financial_month_name` | `dates.fiscal_calendar_month_name as financial_month_name` | Month name for action_at from the shared date dictionary. |
| `week_end_date` | `dates.end_of_iso_week_date as week_end_date` | Sunday ending the ISO week containing action_at, from the shared date dictionary. Independent of session WEEK_START settings. |
| `patient_age` | `a.patient_age as patient_age` | Age in years supplied with the selected action. Null means absent. The available public extract guidance does not establish its calculation date; do not treat it as a freshly calculated age at action or appointment time. |
| `patient_sex_code` | `a.patient_sex_cd::varchar as patient_sex_code` | Patient sex application code supplied on the selected action. Uses the e-RS code system, which differs from national sex codes. |
| `patient_sex_name` | `coalesce(sex.name, nullif(trim(a.patient_sex_desc), '')) as patient_sex_name` | Latest retained Dictionary.E-Referral.Gender display, with supplied-label fallback. This describes the source patient sex field. |
| `residence_lsoa_code` | `nullif(trim(a.patients_lsoa), '') as residence_lsoa_code` | Residence LSOA supplied with the selected action. Preserves source geography rather than the current person address. The feed does not provide an explicit LSOA vintage. |
| `residence_imd_2019_decile` | `imd.imddecile as residence_imd_2019_decile` | IMD 2019 decile matched to the supplied residence LSOA. 1 is most deprived and 10 least deprived. Null means no supplied LSOA or no exact IMD 2019 match. This is an area measure using the named release, not an individual's deprivation or the release current at action time. |
| `registered_practice_code` | `nullif(trim(a.patients_reg_gp_practice_id), '') as registered_practice_code` | Registered GP practice supplied with the selected action, separate from the referring organisation. Not replaced by the person's current practice. |
| `registered_practice_name` | `coalesce(practice.organisation_name, nullif(trim(a.patients_reg_gp_practice_name), '')) as registered_practice_name` | Latest retained organisation name for the supplied practice code, preferring historical UKHFD ODS coverage with Dictionary and source-label fallback. |
| `residence_local_authority_code` | `nullif(trim(a.patients_la_of_residence_id), '') as residence_local_authority_code` | Local authority of residence supplied with the selected action. Source enrichment timing is not established by the public e-RS guidance. |
| `residence_local_authority_name` | `coalesce(residence_la.name, nullif(trim(a.patients_la_of_residence_name), '')) as residence_local_authority_name` | Latest retained Dictionary geography name for the supplied residence local-authority code, with supplied-label fallback. Historical codes remain available. |
| `registration_local_authority_code` | `nullif(trim(a.patients_la_of_registration_id), '') as registration_local_authority_code` | Local authority of GP registration supplied with the selected action. Distinct from residence and referring organisation geography. |
| `registration_local_authority_name` | `coalesce(registration_la.name, nullif(trim(a.patients_la_of_registration_name), '')) as registration_local_authority_name` | Latest retained Dictionary geography name for the supplied registration local-authority code, with supplied-label fallback. |
| `referrer_commissioner_code` | `nullif(trim(a.referrer_commissioner_id), '') as referrer_commissioner_code` | Referrer commissioner code supplied with the selected action. Not an inferred commissioner for the patient's current registration. |
| `referrer_commissioner_name` | `coalesce(commissioner.organisation_name, nullif(trim(a.referrer_commissioner_name), '')) as referrer_commissioner_name` | Latest retained organisation name for the supplied referrer commissioner code, with supplied-label fallback. |
| `action_code` | `a.action_cd::varchar as action_code` | e-RS action code. Includes administrative actions, advice and triage. |
| `action_name` | `coalesce(ac.name, nullif(trim(a.action_desc), '')) as action_name` | Latest retained e-RS dictionary label, falling back to the supplied label when the dictionary has no display. |
| `action_reason_code` | `a.action_reason_cd::varchar as action_reason_code` | Supplied reason for the action. Null when none was recorded. For triage outcomes this identifies the outcome. |
| `action_reason_name` | `coalesce(ar.name, nullif(trim(a.action_reason_desc), '')) as action_reason_name` | Latest retained reason dictionary label, with supplied-label fallback. |
| `priority_code` | `a.priority_cd::varchar as priority_code` | e-RS referral priority code supplied on the action. |
| `priority_name` | `coalesce(pr.name, nullif(trim(a.priority_desc), '')) as priority_name` | Latest retained e-RS priority label, with supplied-label fallback. |
| `specialty_code` | `a.specialty_cd::varchar as specialty_code` | Specialty used in the referral search. Not a national treatment-function code and not necessarily the receiving service specialty. |
| `specialty_name` | `coalesce(sp.name, nullif(trim(a.specialty_desc), '')) as specialty_name` | Latest retained e-RS search specialty label, with supplied-label fallback. |
| `clinic_type_code` | `a.clinic_type_cd::varchar as clinic_type_code` | e-RS clinic type used for the request. |
| `clinic_type_name` | `coalesce(ct.name, nullif(trim(a.clinic_type_desc), '')) as clinic_type_name` | Latest retained clinic-type label, with supplied-label fallback. |
| `referring_organisation_code` | `a.referring_org_id as referring_organisation_code` | ODS code of the referring organisation supplied on the action. |
| `referring_organisation_name` | `coalesce(referrer.organisation_name, nullif(trim(a.referrer_org_name), '')) as referring_organisation_name` | Latest organisation or site name from the shared UKHFD ODS, Dictionary and closed-archive lookup plus e-RS application organisations, with supplied-label fallback. Not a historical name at action time. |
| `action_organisation_code` | `a.org_id as action_organisation_code` | ODS code of the organisation carrying out this action. Distinct from the referrer and provider. |
| `action_organisation_name` | `coalesce(actor.organisation_name, nullif(trim(a.org_name), '')) as action_organisation_name` | Latest organisation or site name from the shared UKHFD ODS, Dictionary and closed-archive lookup plus e-RS application organisations, with supplied-label fallback. Not a historical name at action time. |
| `service_id` | `a.service_id as service_id` | Service associated with this action, where supplied. Different actions for a UBRN may refer to different services. |
| `service_name` | `coalesce(service.service_name, nullif(trim(a.service_name), '')) as service_name` | Latest retained service dictionary name, with supplied-label fallback. Historical service IDs are retained. |
| `service_specialty_code` | `a.service_specialty_cd::varchar as service_specialty_code` | e-RS specialty of the receiving service, distinct from the referral search specialty. |
| `service_specialty_name` | `coalesce(ss.name, nullif(trim(a.service_specialty_desc), '')) as service_specialty_name` | Latest retained service-specialty label, with supplied-label fallback. |
| `provider_organisation_code` | `a.provider_org_id as provider_organisation_code` | ODS code of the service provider supplied on the action. |
| `provider_organisation_name` | `coalesce(provider.organisation_name, nullif(trim(a.provider_org_name), '')) as provider_organisation_name` | Latest organisation or site name from the shared UKHFD ODS, Dictionary and closed-archive lookup plus e-RS application organisations, with supplied-label fallback. Not a historical name at action time. |
| `site_code` | `a.location_org_id as site_code` | ODS code of the service location supplied on the action. |
| `site_name` | `coalesce(site.organisation_name, nullif(trim(a.location_org_name), '')) as site_name` | Latest organisation or site name from the shared UKHFD ODS, Dictionary and closed-archive lookup plus e-RS application organisations, with supplied-label fallback. Not a historical name at action time. |
| `appointment_at` | `a.appt_dt_tm as appointment_at` | Scheduled appointment date and time attached to this action. Several actions may describe the same planned appointment; this is not proof of attendance. |
| `appointment_type_code` | `a.appt_type_cd::varchar as appointment_type_code` | e-RS service appointment type code. |
| `appointment_type_name` | `coalesce(atp.name, nullif(trim(a.appt_type_desc), '')) as appointment_type_name` | Latest retained appointment-type label, with supplied-label fallback. |
| `rebooked_to_action_id` | `a.rebooked_to_action_id as rebooked_to_action_id` | Recorded reference to the booking action for a rebooking. Retained unchanged, including references absent from this extract. Does not invent an old appointment or a cancellation. |
| `initial_ubrn_id` | `a.initial_ubrn_id as initial_ubrn_id` | Initial UBRN source identifier supplied on the action. |
| `previous_ubrn_id` | `a.previous_ubrn_id as previous_ubrn_id` | Previous UBRN source identifier supplied on the action. |
| `initial_ubrn` | `a.initial_ubrn as initial_ubrn` | Supplied initial booking reference, retained unchanged. |
| `previous_ubrn` | `a.previous_ubrn as previous_ubrn` | Supplied previous booking reference, retained unchanged. |
| `next_ubrn` | `a.next_ubrn as next_ubrn` | Supplied next booking reference, retained unchanged. |
| `assessment_outcome_code` | `a.clinical_assessment_outcome_cd::varchar as assessment_outcome_code` | Supplied clinical assessment outcome. Distinct from the action reason used for triage outcomes. |
| `assessment_outcome_name` | `coalesce(ao.name, nullif(trim(a.clinical_assess_outcome_desc), '')) as assessment_outcome_name` | Latest retained assessment-outcome label, with supplied-label fallback. |
| `advice_request_status_code` | `a.ar_status_cd::varchar as advice_request_status_code` | Advice-request status supplied on the action. |
| `advice_request_status_name` | `nullif(trim(a.ar_status_desc), '') as advice_request_status_name` | Supplied label for advice-request status. |
| `source_submission_id` | `a.uniq_submission_id as source_submission_id` | Source submission active when the fact was refreshed. |
| `source_imported_at` | `a.dmic_date_added as source_imported_at` | Source import time. Separate from clinical or action time. |

## fct_ers_referral

| Column | Selected expression | Meaning |
|---|---|---|
| `ubrn_id` | `h.ubrn_id` | One e-RS request identifier. Includes advice and triage requests; an initial Referral Created action is not required. |
| `ubrn` | `a.ubrn` | Supplied booking reference, preserved unchanged. |
| `normalised_ubrn` | `a.normalised_ubrn` | Booking reference with display hyphens and surrounding spaces removed. Use for recorded cross-source matching and also check patient-key agreement. |
| `sk_patient_id` | `h.sk_patient_id` | Shared patient key when all non-null keys in the action history agree. Null when missing or conflicting; the referral remains available. |
| `patient_age` | `a.patient_age` | Age in years supplied with the selected action. Null means absent. The available public extract guidance does not establish its calculation date; do not treat it as a freshly calculated age at action or appointment time. Selected action means latest_action_id, the highest sequential action ID for this request. It does not mean the creation action. |
| `patient_sex_code` | `a.patient_sex_code` | Patient sex application code supplied on the selected action. Uses the e-RS code system, which differs from national sex codes. Selected action means latest_action_id, the highest sequential action ID for this request. It does not mean the creation action. |
| `patient_sex_name` | `a.patient_sex_name` | Latest retained Dictionary.E-Referral.Gender display, with supplied-label fallback. This describes the source patient sex field. Selected action means latest_action_id, the highest sequential action ID for this request. It does not mean the creation action. |
| `residence_lsoa_code` | `a.residence_lsoa_code` | Residence LSOA supplied with the selected action. Preserves source geography rather than the current person address. The feed does not provide an explicit LSOA vintage. Selected action means latest_action_id, the highest sequential action ID for this request. It does not mean the creation action. |
| `residence_imd_2019_decile` | `a.residence_imd_2019_decile` | IMD 2019 decile matched to the supplied residence LSOA. 1 is most deprived and 10 least deprived. Null means no supplied LSOA or no exact IMD 2019 match. This is an area measure using the named release, not an individual's deprivation or the release current at action time. Selected action means latest_action_id, the highest sequential action ID for this request. It does not mean the creation action. |
| `registered_practice_code` | `a.registered_practice_code` | Registered GP practice supplied with the selected action, separate from the referring organisation. Not replaced by the person's current practice. Selected action means latest_action_id, the highest sequential action ID for this request. It does not mean the creation action. |
| `registered_practice_name` | `a.registered_practice_name` | Latest retained organisation name for the supplied practice code, preferring historical UKHFD ODS coverage with Dictionary and source-label fallback. Selected action means latest_action_id, the highest sequential action ID for this request. It does not mean the creation action. |
| `residence_local_authority_code` | `a.residence_local_authority_code` | Local authority of residence supplied with the selected action. Source enrichment timing is not established by the public e-RS guidance. Selected action means latest_action_id, the highest sequential action ID for this request. It does not mean the creation action. |
| `residence_local_authority_name` | `a.residence_local_authority_name` | Latest retained Dictionary geography name for the supplied residence local-authority code, with supplied-label fallback. Historical codes remain available. Selected action means latest_action_id, the highest sequential action ID for this request. It does not mean the creation action. |
| `registration_local_authority_code` | `a.registration_local_authority_code` | Local authority of GP registration supplied with the selected action. Distinct from residence and referring organisation geography. Selected action means latest_action_id, the highest sequential action ID for this request. It does not mean the creation action. |
| `registration_local_authority_name` | `a.registration_local_authority_name` | Latest retained Dictionary geography name for the supplied registration local-authority code, with supplied-label fallback. Selected action means latest_action_id, the highest sequential action ID for this request. It does not mean the creation action. |
| `referrer_commissioner_code` | `a.referrer_commissioner_code` | Referrer commissioner code supplied with the selected action. Not an inferred commissioner for the patient's current registration. Selected action means latest_action_id, the highest sequential action ID for this request. It does not mean the creation action. |
| `referrer_commissioner_name` | `a.referrer_commissioner_name` | Latest retained organisation name for the supplied referrer commissioner code, with supplied-label fallback. Selected action means latest_action_id, the highest sequential action ID for this request. It does not mean the creation action. |
| `pathway_started_at` | `h.pathway_started_at` | Latest non-null pathway-start value by sequential action ID. Not inferred from the first observed action. |
| `first_observed_action_at` | `h.first_observed_action_at` | Earliest recorded action time present in the active extract. History may begin after the request was created. |
| `last_observed_action_at` | `h.last_observed_action_at` | Latest recorded action time present in the active extract. |
| `action_count` | `h.action_count` | Number of active submitted actions, including administrative actions. |
| `first_action_id` | `h.first_action_id` | Lowest sequential action ID available for this request. |
| `latest_action_id` | `h.latest_action_id` | Highest sequential action ID available for this request. Defines the latest request attributes. |
| `latest_action_code` | `a.action_code as latest_action_code` | Most recent recorded action code, including administrative actions. It is not a derived referral status. |
| `latest_action_name` | `a.action_name as latest_action_name` | Label for the most recent recorded action; not a derived open, closed or attendance state. |
| `latest_action_reason_code` | `a.action_reason_code as latest_action_reason_code` | Reason attached to the latest recorded action. |
| `latest_action_reason_name` | `a.action_reason_name as latest_action_reason_name` | Label for the reason attached to the latest recorded action. |
| `priority_code` | `a.priority_code` | e-RS referral priority code supplied on the action. |
| `priority_name` | `a.priority_name` | Latest retained e-RS priority label, with supplied-label fallback. |
| `specialty_code` | `a.specialty_code` | Specialty used in the referral search. Not a national treatment-function code and not necessarily the receiving service specialty. |
| `specialty_name` | `a.specialty_name` | Latest retained e-RS search specialty label, with supplied-label fallback. |
| `clinic_type_code` | `a.clinic_type_code` | e-RS clinic type used for the request. |
| `clinic_type_name` | `a.clinic_type_name` | Latest retained clinic-type label, with supplied-label fallback. |
| `referring_organisation_code` | `a.referring_organisation_code` | ODS code of the referring organisation supplied on the action. |
| `referring_organisation_name` | `a.referring_organisation_name` | Referrer name supplied by the enriched feed. Not a guaranteed historical name. |
| `recorded_service_count` | `h.recorded_service_count` | Number of distinct services appearing in the action history. A referral can involve several services. |
| `last_service_action_id` | `h.last_service_action_id` | Highest action ID carrying a service. Identifies the source of all last-recorded service attributes. |
| `last_service_action_at` | `s.action_at as last_service_action_at` | Recorded time of the service-bearing action. Later administrative or cancellation actions may exist. |
| `last_recorded_service_id` | `s.service_id as last_recorded_service_id` | Last recorded service id, taken with the other service fields from last_service_action_id. Does not establish the current destination or provider. |
| `last_recorded_service_name` | `s.service_name as last_recorded_service_name` | Last recorded service name, taken with the other service fields from last_service_action_id. Does not establish the current destination or provider. |
| `last_recorded_service_specialty_code` | `s.service_specialty_code as last_recorded_service_specialty_code` | Last recorded service specialty code, taken with the other service fields from last_service_action_id. Does not establish the current destination or provider. |
| `last_recorded_service_specialty_name` | `s.service_specialty_name as last_recorded_service_specialty_name` | Last recorded service specialty name, taken with the other service fields from last_service_action_id. Does not establish the current destination or provider. |
| `last_recorded_provider_code` | `s.provider_organisation_code as last_recorded_provider_code` | Last recorded provider code, taken with the other service fields from last_service_action_id. Does not establish the current destination or provider. |
| `last_recorded_provider_name` | `s.provider_organisation_name as last_recorded_provider_name` | Last recorded provider name, taken with the other service fields from last_service_action_id. Does not establish the current destination or provider. |
| `last_recorded_site_code` | `s.site_code as last_recorded_site_code` | Last recorded site code, taken with the other service fields from last_service_action_id. Does not establish the current destination or provider. |
| `last_recorded_site_name` | `s.site_name as last_recorded_site_name` | Last recorded site name, taken with the other service fields from last_service_action_id. Does not establish the current destination or provider. |
| `initial_ubrn_id` | `a.initial_ubrn_id` | Initial UBRN source identifier supplied on the action. |
| `previous_ubrn_id` | `a.previous_ubrn_id` | Previous UBRN source identifier supplied on the action. |
| `initial_ubrn` | `a.initial_ubrn` | Supplied initial booking reference, retained unchanged. |
| `previous_ubrn` | `a.previous_ubrn` | Supplied previous booking reference, retained unchanged. |
| `next_ubrn` | `a.next_ubrn` | Supplied next booking reference, retained unchanged. |

## fct_ers_appointment

| Column | Selected expression | Meaning |
|---|---|---|
| `appointment_id` | `md5(cast(coalesce(cast(h.ubrn_id as TEXT), '_dbt_utils_surrogate_key_null_') \|\| '-' \|\| coalesce(cast(h.service_id as TEXT), '_dbt_utils_surrogate_key_null_') \|\| '-' \|\| coalesce(cast(to_char(h.appointment_at, 'YYYY-MM-DD HH24:MI:SS.FF9') as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as appointment_id` | Stable key of UBRN identifier, service and scheduled time. Timestamp formatting is explicit so the key does not depend on session display settings. |
| `ubrn_id` | `h.ubrn_id` | Source identifier of the referral, triage or advice request. |
| `ubrn` | `a.ubrn` | Supplied booking reference, preserved unchanged. |
| `sk_patient_id` | `h.sk_patient_id` | Shared pseudonymised NHS-number key when all non-null keys in the appointment's action history agree. Missing keys on some actions do not invalidate an agreed key. Null when every key is missing or non-null keys conflict; no demographic or registration filter is applied. |
| `appointment_at` | `h.appointment_at` | Recorded scheduled date and time. A planned appointment is not proof of attendance. |
| `service_id` | `h.service_id` | Service associated with this action, where supplied. Different actions for a UBRN may refer to different services. |
| `service_name` | `a.service_name` | Latest retained service dictionary name, with supplied-label fallback. Historical service IDs are retained. |
| `service_specialty_code` | `a.service_specialty_code` | e-RS specialty of the receiving service, distinct from the referral search specialty. |
| `service_specialty_name` | `a.service_specialty_name` | Latest retained service-specialty label, with supplied-label fallback. |
| `provider_organisation_code` | `a.provider_organisation_code` | ODS code of the service provider supplied on the action. |
| `provider_organisation_name` | `a.provider_organisation_name` | Latest organisation or site name from the shared UKHFD ODS, Dictionary and closed-archive lookup plus e-RS application organisations, with supplied-label fallback. Not a historical name at action time. |
| `site_code` | `a.site_code` | ODS code of the service location supplied on the action. |
| `site_name` | `a.site_name` | Latest organisation or site name from the shared UKHFD ODS, Dictionary and closed-archive lookup plus e-RS application organisations, with supplied-label fallback. Not a historical name at action time. |
| `appointment_type_code` | `a.appointment_type_code` | e-RS service appointment type code. |
| `appointment_type_name` | `a.appointment_type_name` | Latest retained appointment-type label, with supplied-label fallback. |
| `action_count` | `h.action_count` | Number of contributing actions with this UBRN, service and scheduled time, including administrative actions. |
| `recorded_booking_action_count` | `h.recorded_booking_action_count` | Number of Appointment Booked actions (1412). Repeated booking actions for the same slot do not create additional appointments. Zero means no such action is present in the available history. |
| `first_observed_action_at` | `h.first_observed_action_at` | Earliest action time attached to this planned appointment. Not necessarily when it was booked. |
| `last_observed_action_at` | `h.last_observed_action_at` | Latest action time attached to this planned appointment. |
| `latest_action_id` | `h.latest_action_id` | Highest contributing action ID. All service and organisation fields come together from that action. |
| `latest_appointment_action_id` | `h.latest_appointment_action_id` | Highest action ID for booking, cancellation, displacement, rebooking, rebooking instructions, acceptance, rejection or DNA. Later administrative actions do not replace it. |
| `latest_appointment_action_at` | `status.action_at as latest_appointment_action_at` | Time of the latest appointment-related action, where one is present. |
| `latest_appointment_action_code` | `status.action_code as latest_appointment_action_code` | Latest appointment-related e-RS action code. Retains cancellation-not-confirmed, DNA-resolved and referral acceptance as distinct actions. Does not infer attendance, completion or a simplified current state. |
| `latest_appointment_action_name` | `status.action_name as latest_appointment_action_name` | Reference label for the latest appointment-related action. Null if only other actions describe this appointment. |
| `latest_appointment_action_reason_code` | `status.action_reason_code as latest_appointment_action_reason_code` | Reason attached to the latest appointment-related action. |
| `latest_appointment_action_reason_name` | `status.action_reason_name as latest_appointment_action_reason_name` | Label for that recorded reason. |

## rel_ers_appointment_action

| Column | Selected expression | Meaning |
|---|---|---|
| `appointment_id` | `p.appointment_id` | Consolidated planned appointment key in fct_ers_appointment. |
| `action_id` | `a.action_id` | Source action ID in fct_ers_referral_action. |

## rel_ers_referral_acute_record

| Column | Selected expression | Meaning |
|---|---|---|
| `ubrn_id` | `e.ubrn_id` | Matched request identifier in fct_ers_referral. |
| `acute_source` | `a.acute_source` | SUS dataset owning the source record: outpatient appointment or admitted-patient spell. |
| `acute_source_record_id` | `a.acute_source_record_id` | PRIMARYKEY_ID in stg_sus_op_appointment or stg_sus_apc_spell, interpreted with acute_source. |
| `acute_record_date` | `a.acute_record_date` | Outpatient scheduled date or admitted-patient admission date. Not an inferred referral milestone. |
| `acute_booking_reference` | `a.acute_booking_reference` | Pseudonymised UBRN supplied on the SUS record, with original presentation retained. |
| `normalised_ubrn` | `e.normalised_ubrn` | Recorded UBRN with surrounding whitespace and display hyphens removed. No date or person-based matching is used. |
| `ers_sk_patient_id` | `e.sk_patient_id as ers_sk_patient_id` | Shared patient key on the e-RS request. Kept separate from the acute key so disagreements remain visible. |
| `acute_sk_patient_id` | `a.acute_sk_patient_id` | Shared patient key on the acute source record. |
| `patient_key_agreement` | `case when e.sk_patient_id is null or a.acute_sk_patient_id is null then 'missing_key' when e.sk_patient_id = a.acute_sk_patient_id then 'agree' else 'mismatch' end as patient_key_agreement` | agree when both supplied patient keys are present and equal; mismatch when both are present and differ; missing_key when either is absent. A reference match with mismatch or missing_key must not be treated as a confirmed person-level link. Even agreement does not prove causality. |
