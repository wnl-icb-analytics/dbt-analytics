{% docs navigation_event_id %}
Stable namespaced identifier for one source milestone. OLIDS retains its upstream UUID without a text prefix. A date correction keeps the same event ID; this is not an audit version ID.
{% enddocs %}

{% docs navigation_clinical_record_id %}
Stable namespaced identifier for one clinical source record. OLIDS retains its upstream UUID without a text prefix in both outputs. On the event output, this is an explicitly linked clinical record, not a temporal match.
{% enddocs %}

{% docs navigation_sk_patient_id %}
Cross-system pseudonymised NHS number key, produced with the approved salt and pepper. Use this key for person lookups; many clinical records and events can share it. It is not a unique row key. Missing linkage does not exclude a record. Never substitute a source person or practice patient ID.
{% enddocs %}

{% docs navigation_source_person_id %}
Person identifier within the named source dataset. OLIDS uses its practice-consistent person_id here. Other datasets have separate namespaces. For secondary use, filter OLIDS rows through dim_person_secondary_use_allowed using this ID.
{% enddocs %}

{% docs navigation_source_dataset %}
Dataset namespace: OLIDS, MHSDS, CSDS, SUS_APC, SUS_OP, ECDS or ERS. OLIDS currently covers the filtered NCL spine; other feeds retain their available WNL source population.
{% enddocs %}

{% docs navigation_source_record_type %}
Source entity within its dataset. Use together with source_record_id and source_model_name to find the detailed record.
{% enddocs %}

{% docs navigation_source_record_id %}
Logical record key in the named source model. See the source-key mapping in the longitudinal activity guide. OLIDS clinical detail also uses source_record_type. Delivery dates and event dates do not form milestone IDs.
{% enddocs %}

{% docs navigation_source_model_name %}
dbt model containing the source detail. Most keys are source_record_id; appointments, e-RS and acute staging use their documented entity keys.
{% enddocs %}

{% docs navigation_event_date %}
Recorded event date. Interpret with event_time_precision: month and year values are partial dates, and unknown precision does not establish an exact day. Retained future dates do not prove that planned care happened.
{% enddocs %}

{% docs navigation_event_at %}
Timeline sorting timestamp. Retains an established source clock time; otherwise uses midnight on the recorded date, or the first day of a recorded month or year. These anchors are presentation conventions, not observed times. Null only when no source date is available. Interpret with event_time_precision.
{% enddocs %}

{% docs navigation_event_time_precision %}
Recorded precision: timestamp, date, month, year or unknown. Date-only records belong on their day but do not establish within-day ordering.
{% enddocs %}

{% docs navigation_event_time_basis %}
Source field defining the milestone date. This explains what happened at that time; delivery timestamps are never used as clinical event dates.
{% enddocs %}

{% docs navigation_event_type %}
Shared milestone type. Bookings and scheduled slots are separate events. Neither establishes attendance. Emergency departure, referral discharge and hospital discharge retain their distinct meanings.
{% enddocs %}

{% docs navigation_event_name %}
Readable milestone label. e-RS preserves the recorded action label, including automatic acceptance and separate DNA updates.
{% enddocs %}

{% docs navigation_event_code %}
Recorded code associated with the milestone. For an e-RS slot this is its latest recorded appointment-state action, not evidence of attendance.
{% enddocs %}

{% docs navigation_event_code_name %}
Authoritative or retained source label for event_code.
{% enddocs %}

{% docs navigation_event_coding_system %}
Namespace defining event_code, such as the e-RS action dictionary or the supplied OLIDS clinical coding system.
{% enddocs %}

{% docs navigation_care_setting %}
Broad source care setting. e-RS does not imply an attended care setting, so its value is null.
{% enddocs %}

{% docs navigation_care_setting_name %}
Readable name for the broad source care setting.
{% enddocs %}

{% docs navigation_status_code %}
Recorded source status code. Meanings remain source-specific; referral acceptance does not mean attendance.
{% enddocs %}

{% docs navigation_status_name %}
Retained source or reference label for status_code.
{% enddocs %}

{% docs navigation_attendance_code %}
Recorded attendance status. A null value means no attendance status is supplied in this branch.
{% enddocs %}

{% docs navigation_attendance_name %}
Authoritative reference label for attendance_code.
{% enddocs %}

{% docs navigation_outcome_code %}
Recorded source outcome or action-reason code. Interpret within the source dataset and record type.
{% enddocs %}

{% docs navigation_outcome_name %}
Retained source or reference label for outcome_code.
{% enddocs %}

{% docs navigation_service_or_team_type_code %}
Recorded service or team category, where supplied by the source. Not inferred from nearby records.
{% enddocs %}

{% docs navigation_service_or_team_type_name %}
Latest retained authoritative label for the service or team category.
{% enddocs %}

{% docs navigation_specialty_code %}
Recorded specialty code, interpreted within the source dataset. e-RS action specialty and slot service specialty retain their source meanings.
{% enddocs %}

{% docs navigation_specialty_name %}
Retained source or reference label for specialty_code.
{% enddocs %}

{% docs navigation_consultation_mechanism_code %}
Recorded contact mechanism, such as face-to-face or telephone, in its source code system.
{% enddocs %}

{% docs navigation_consultation_mechanism_name %}
Latest retained authoritative label for the consultation mechanism.
{% enddocs %}

{% docs navigation_activity_location_type_code %}
Source category for the location of a care contact.
{% enddocs %}

{% docs navigation_activity_location_type_name %}
Latest retained authoritative label for the activity location category.
{% enddocs %}

{% docs navigation_provider_organisation_code %}
Recorded provider code. Interpret with provider_code_authority; an OLIDS local identifier is not automatically an ODS code.
{% enddocs %}

{% docs navigation_provider_organisation_name %}
Retained source or reference name for the recorded provider. A latest reference label does not establish a historical organisation relationship.
{% enddocs %}

{% docs navigation_provider_code_authority %}
Authority assigning the provider code. National submitted codes use ODS; OLIDS retains its supplied authority.
{% enddocs %}

{% docs navigation_site_code %}
Recorded treatment or appointment site code, without replacing it with a provider code.
{% enddocs %}

{% docs navigation_site_name %}
Retained source or reference name for the recorded site. Null when the code has no supported site label.
{% enddocs %}

{% docs navigation_referring_organisation_code %}
Recorded referring organisation, where available. No referral direction is inferred from time proximity.
{% enddocs %}

{% docs navigation_referring_organisation_name %}
Retained source or reference name for the referring organisation.
{% enddocs %}

{% docs navigation_parent_record_type %}
Entity type of the explicit recorded parent. Other submitted links and consistency flags remain on the detailed source fact.
{% enddocs %}

{% docs navigation_parent_record_id %}
Recorded parent key in parent_model_name. A recorded link does not establish causality, attendance or a journey. Clinical activity links with conflicting person identifiers are not promoted.
{% enddocs %}

{% docs navigation_parent_model_name %}
dbt model containing the recorded parent. Consult the source-key mapping for keys named appointment_id, visit_occurrence_id, ubrn_id or id.
{% enddocs %}

{% docs navigation_relationship_type %}
recorded_parent means the link comes from source identifiers. No person-and-time association is published here.
{% enddocs %}

{% docs navigation_source_submission_period %}
Reporting period end for the selected MHSDS or CSDS submission. It is not the event date.
{% enddocs %}

{% docs navigation_source_received_at %}
Recorded receipt, import or extraction timestamp used for incremental loading. It describes source delivery, not clinical activity. Receipts predating the stored watermark are reconciled at the monthly full refresh.
{% enddocs %}

{% docs navigation_clinical_record_type %}
Source clinical item type. An OLIDS observation can contain diagnoses, allergies or referral content; its code defines the meaning. Medication rows are orders enriched by statements, with no standalone statements.
{% enddocs %}

{% docs navigation_clinical_record_type_name %}
Readable name for the source clinical item type.
{% enddocs %}

{% docs navigation_clinical_record_date %}
Recorded or source-validated clinical date. SUS diagnoses and other undated codes remain undated; an admission or appointment date is not substituted. Interpret partial OLIDS dates with clinical_time_precision.
{% enddocs %}

{% docs navigation_clinical_record_at %}
Timeline sorting timestamp. Retains an established source clock time; otherwise uses midnight on the clinical date, or the first day of a recorded month or year. These anchors do not establish within-day sequence or an exact day for a partial date. Undated records remain null; recording and encounter dates do not replace the clinical date.
{% enddocs %}

{% docs navigation_clinical_time_precision %}
timestamp, date, month, year or unknown. Source day precision cannot establish ordering within that day. MHSDS stored timestamps with unverified clock precision have date precision here. Their detailed source fact retains the original timestamp and supplied date; is_source_date_inconsistent identifies disagreements. Unknown precision with a populated date retains the source date as a sorting anchor without claiming its precision.
{% enddocs %}

{% docs navigation_clinical_time_basis %}
Source field or validated recorded relationship supplying the clinical date. not_recorded means no supported date exists. Parent episode dates are not substituted for missing diagnosis dates.
{% enddocs %}

{% docs navigation_source_code %}
Code supplied for the clinical item, with the source model's documented normalisation. Use source_coding_system to interpret it.
{% enddocs %}

{% docs navigation_source_code_name %}
Latest supported reference or source description of the clinical code. Historical codes remain valid in their recorded history; no terminology service is called at query time.
{% enddocs %}

{% docs navigation_source_coding_system %}
Source clinical coding system or its source-supplied label. Keep source and mapped codes separate.
{% enddocs %}

{% docs navigation_mapped_code %}
SNOMED CT code for analysis. A recognised source SNOMED code is used directly, including inactive historical concepts. MHSDS, CSDS and ECDS validate candidates against the NHS Digital concept reference when loaded; supported Read mappings can supply a target. OLIDS retains its prepared EMIS mapping, including targets not recognised in that reference. ICD-10 and OPCS-4 reverse-map candidates are not assigned as clinical equivalents. Null means no mapping was resolved; the source code remains available. Monthly full refreshes apply reference changes to older deliveries.
{% enddocs %}

{% docs navigation_mapped_code_name %}
Preferred reference label for mapped_code. MHSDS, CSDS and ECDS use the latest retained NHS Digital term, including for historical concepts. OLIDS retains its prepared mapping label.
{% enddocs %}

{% docs navigation_mapped_coding_system %}
Coding system defining mapped_code. No mapping is inferred in the shared union.
{% enddocs %}

{% docs navigation_result_value %}
Recorded clinical result as text, including numeric results and enumerated responses. Medication dose and quantity have separate fields.
{% enddocs %}

{% docs navigation_result_value_name %}
Published response label where a clinical assessment has an enumerated response. Null does not imply an invalid numeric value.
{% enddocs %}

{% docs navigation_result_value_numeric %}
Numeric representation at precision 38 and scale 9. Check result_value_parse_status before analysis; an enumerated non-score response is not necessarily a usable assessment score. Use assessment_score_numeric and assessment_response_status for assessment scoring.
{% enddocs %}

{% docs navigation_result_value_parse_status %}
Source numeric parsing assessment. Rounded or out-of-range values remain identifiable, and the original result_value is retained.
{% enddocs %}

{% docs navigation_result_date %}
Date-valued OLIDS result. This is the result content, not a replacement clinical event date.
{% enddocs %}

{% docs navigation_result_unit_code %}
Recorded measurement-unit code or source unit token. Interpret with its label and detailed source provenance.
{% enddocs %}

{% docs navigation_result_unit_name %}
Supported unit label. CSDS historical aliases apply only to their documented matching measurement codes; the source fact retains definition provenance.
{% enddocs %}

{% docs navigation_result_unit_symbol %}
Supported unit symbol where supplied by the prepared source model.
{% enddocs %}

{% docs navigation_medication_name %}
Source medication name for an order or medication-related allergy observation. This field alone does not classify a record as prescribing.
{% enddocs %}

{% docs navigation_medication_dose %}
Dose supplied on the medication order. Statement doses do not overwrite it.
{% enddocs %}

{% docs navigation_medication_quantity_value %}
Quantity supplied on the medication order, interpreted with medication_quantity_unit.
{% enddocs %}

{% docs navigation_medication_quantity_unit %}
Unit supplied for the medication order quantity.
{% enddocs %}

{% docs navigation_medication_duration_days %}
Order duration in days, where supplied.
{% enddocs %}

{% docs navigation_medication_authorisation_type_code %}
Authorisation type from the currently linked, non-deleted statement for the same OLIDS person. It is context, not historical authorisation evidence.
{% enddocs %}

{% docs navigation_medication_authorisation_type_name %}
Source label for the linked statement authorisation type.
{% enddocs %}

{% docs navigation_qualifier_code %}
Recorded ECDS diagnosis qualifier, such as confirmed or suspected, in SNOMED CT.
{% enddocs %}

{% docs navigation_qualifier_name %}
Retained SNOMED CT label for the recorded diagnosis qualifier.
{% enddocs %}
