-- Aggregate-only DEV validation of the current booking snapshot.
SELECT COUNT(*) AS row_count, COUNT(DISTINCT appointment_id) AS distinct_appointments,
OBJECT_CONSTRUCT('appointment_id', OBJECT_CONSTRUCT('non_null', COUNT(appointment_id), 'blank', COUNT_IF(TRIM(appointment_id::VARCHAR) = '')),
'person_id', OBJECT_CONSTRUCT('non_null', COUNT(person_id), 'blank', COUNT_IF(TRIM(person_id::VARCHAR) = '')),
'sk_patient_id', OBJECT_CONSTRUCT('non_null', COUNT(sk_patient_id), 'blank', COUNT_IF(TRIM(sk_patient_id::VARCHAR) = '')),
'patient_id', OBJECT_CONSTRUCT('non_null', COUNT(patient_id), 'blank', COUNT_IF(TRIM(patient_id::VARCHAR) = '')),
'booked_at', OBJECT_CONSTRUCT('non_null', COUNT(booked_at), 'blank', COUNT_IF(TRIM(booked_at::VARCHAR) = '')),
'scheduled_at', OBJECT_CONSTRUCT('non_null', COUNT(scheduled_at), 'blank', COUNT_IF(TRIM(scheduled_at::VARCHAR) = '')),
'source_booking_method_code', OBJECT_CONSTRUCT('non_null', COUNT(source_booking_method_code), 'blank', COUNT_IF(TRIM(source_booking_method_code::VARCHAR) = '')),
'source_booking_method_name', OBJECT_CONSTRUCT('non_null', COUNT(source_booking_method_name), 'blank', COUNT_IF(TRIM(source_booking_method_name::VARCHAR) = '')),
'booking_method_code', OBJECT_CONSTRUCT('non_null', COUNT(booking_method_code), 'blank', COUNT_IF(TRIM(booking_method_code::VARCHAR) = '')),
'booking_method_name', OBJECT_CONSTRUCT('non_null', COUNT(booking_method_name), 'blank', COUNT_IF(TRIM(booking_method_name::VARCHAR) = '')),
'provider_organisation_id', OBJECT_CONSTRUCT('non_null', COUNT(provider_organisation_id), 'blank', COUNT_IF(TRIM(provider_organisation_id::VARCHAR) = '')),
'provider_organisation_code', OBJECT_CONSTRUCT('non_null', COUNT(provider_organisation_code), 'blank', COUNT_IF(TRIM(provider_organisation_code::VARCHAR) = '')),
'provider_organisation_code_authority', OBJECT_CONSTRUCT('non_null', COUNT(provider_organisation_code_authority), 'blank', COUNT_IF(TRIM(provider_organisation_code_authority::VARCHAR) = '')),
'provider_organisation_name', OBJECT_CONSTRUCT('non_null', COUNT(provider_organisation_name), 'blank', COUNT_IF(TRIM(provider_organisation_name::VARCHAR) = '')),
'publisher_organisation_id', OBJECT_CONSTRUCT('non_null', COUNT(publisher_organisation_id), 'blank', COUNT_IF(TRIM(publisher_organisation_id::VARCHAR) = '')),
'publisher_organisation_code', OBJECT_CONSTRUCT('non_null', COUNT(publisher_organisation_code), 'blank', COUNT_IF(TRIM(publisher_organisation_code::VARCHAR) = '')),
'publisher_organisation_name', OBJECT_CONSTRUCT('non_null', COUNT(publisher_organisation_name), 'blank', COUNT_IF(TRIM(publisher_organisation_name::VARCHAR) = ''))) AS completeness
FROM DEV__REPORTING.OLIDS_APPOINTMENTS.FCT_GP_APPOINTMENT_BOOKING;
WITH actual AS (
SELECT COUNT(*) AS row_count, HASH_AGG(appointment_id,person_id,sk_patient_id,patient_id,booked_at,scheduled_at,source_booking_method_code,source_booking_method_name,booking_method_code,booking_method_name,provider_organisation_id,provider_organisation_code,provider_organisation_code_authority,provider_organisation_name,publisher_organisation_id,publisher_organisation_code,publisher_organisation_name) AS output_hash
FROM DEV__REPORTING.OLIDS_APPOINTMENTS.FCT_GP_APPOINTMENT_BOOKING
), expected AS (
SELECT COUNT(*) AS row_count, HASH_AGG(appointment_id,person_id,sk_patient_id,patient_id,booked_at,scheduled_at,source_booking_method_code,source_booking_method_name,booking_method_code,booking_method_name,provider_organisation_id,provider_organisation_code,provider_organisation_code_authority,provider_organisation_name,publisher_organisation_id,publisher_organisation_code,publisher_organisation_name) AS output_hash
FROM DEV__REPORTING.OLIDS_APPOINTMENTS.FCT_GP_APPOINTMENT WHERE booked_at IS NOT NULL
)
SELECT actual.row_count AS actual_count, expected.row_count AS expected_count,
actual.output_hash = expected.output_hash AS all_columns_match
FROM actual CROSS JOIN expected;
