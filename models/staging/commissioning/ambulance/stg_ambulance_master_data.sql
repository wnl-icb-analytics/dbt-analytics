{{
    config(
        materialized='table',
        tags=['LAS', 'AMB'],
          )
}}


/*
Purpose:
Staging model for the London Ambulance Service dataset.

This model prepares the Ambulance source data (DATA_LAKE.SDL.AMB source table),
for downstream modelling and reporting.

Grain:
One row per ambulance incident.

Key Features:
• Retains all records from the source table
• Includes only columns required for downstream modelling and reporting
• Standardises column names to ICB snake-case naming convention
• Converts dates and timestamps to the correct data types.
• Removes unused columns.

Inclusion Criteria:
• All records from MASTER_AMB_DATA.

Known Limitations:
• Some fields are awaiting confirmation from the legacy reporting model.
*/

SELECT
    -------------------------------------------------------------------------
    -- Data Lake Metadata
    -------------------------------------------------------------------------
      "DLP_FLEXOR_FREEZE" as dlp_flex_or_freeze                
    , "DLP_COMMISSIONER_CODE" as dlp_commissioner_code
    , "DLP_FINANCIAL_MONTH" as dlp_financial_month
    , "DLP_FINANCIAL_YEAR" as dlp_financial_year
    , "DLP_BASELINE_FINANCIAL_MONTH" as dlp_baseline_financial_month

    -------------------------------------------------------------------------
    -- Incident Identifiers
    -------------------------------------------------------------------------
    , "INCIDENT_ID" as incident_id
    , "CAD_NUMBER" as cad_number

    -------------------------------------------------------------------------
    -- Dates and Times
    -------------------------------------------------------------------------
    , try_to_date("DATE_VALUE") as date_value  
    , try_to_timestamp_ntz("CALL_TIME") as call_time
    , try_to_timestamp_ntz("CLOCK_START_TIME") as clock_start_time
    , try_to_timestamp_ntz("ARRIVE_ON_SCENE_FIRST_VEHICLE") as arrive_on_scene_first_vehicle
    , try_to_timestamp_ntz("ARRIVE_ON_SCENE_FIRST_TRANSPORTING_VEHICLE") as arrive_on_scene_first_transporting_vehicle
    , try_to_timestamp_ntz("CONVEYING_DATETIME") as conveying_datetime
    , try_to_timestamp_ntz("PRE_TRIAGED_QUESTIONS_TIME") as pre_triaged_questions_time

    -------------------------------------------------------------------------
    -- Geography & Organisation
    -------------------------------------------------------------------------
    , "LOCALITY" as locality
    , "ICB" as icb
    , "CROSS_BORDER_FLAG" as cross_border_flag
    , "DV_LSOA" as dv_lsoa
    , "DV_PARTIAL_POST_CODE" as dv_partial_post_code
    , "DV_PARTIAL_POST_CODE_DESTINATION_POSTCODE" as dv_partial_post_code_destination_postcode
    , "DV_RECIPIENT_CODE" as dv_recipient_code
    , "NHS_SITECODE" as nhs_sitecode
    , "GP_LOCATION" as gp_location

    -------------------------------------------------------------------------
    -- Patient Demographics
    -------------------------------------------------------------------------
    , "CALL_AGE" as call_age
    , "CALL_GENDER" as call_gender
    , "SK_PATIENT_ID_NHS_NUMBER" as sk_patient_id_nhs_number
    , "CARE_HOME_LOCATION_FLAG" as care_home_location_flag

    -------------------------------------------------------------------------
    -- Clinical Information
    -------------------------------------------------------------------------
    , "DETERMINANT_CODE" as determinant_code
    , "CHIEF_COMPLAINT_CODE" as chief_complaint_code
    , "AMPDS_CODE_VERSION" as ampds_code_version
    , "INCIDENT_CODE" as incident_code
    , "ILLNESS_CODE_1" as illness_code_1
    , "ILLNESS_CODE_2" as illness_code_2
    , "ILLNESS_INCIDENT_CODE_VERSION" as illness_incident_code_version
    , "DO_H_CATEGORY" as do_h_category
    , "FULL_DOH_CATEGORY" as full_doh_category
    , "SUB_CATEGORY" as sub_category
    , "CALL_HRG_CODE" as call_hrg_code
    , "INCIDENT_HRG_CODE" as incident_hrg_code
    , "HRG_VERSION_NUMBER" as hrg_version_number
    , "PRE_TRIAGED_QUESTIONS" as pre_triaged_questions
    , "ADDITIONAL_H_T" as additional_h_t
    -------------------------------------------------------------------------
    -- Operational Information
    -------------------------------------------------------------------------
    , "CALL_TYPE" as call_type
    , "DESTINATION_REFERRAL_OUTCOME" as destination_referral_outcome
    , "MODE_OF_TRANSPORT_FLAG" as mode_of_transport_flag
    , "MODE_OF_TRANSPORT_FIRST_RESPONDER" as mode_of_transport_first_responder
    , "CONVEYING_CREW_TYPE" as conveying_crew_type
    , "FIRST_RESPONDER_CREW_TYPE" as first_responder_crew_type
    , "DEPARTMENT_TYPE_FLAG" as department_type_flag
    , "PRE_ALERT_BLUE_CALL_FLAG" as pre_alert_blue_call_flag
    , "H_T_TAXI_FLAG" as h_t_taxi_flag
    , "BASELINE_SETTING_INCLUSIONS" as baseline_setting_inclusions

    -------------------------------------------------------------------------
    -- Mental Health / Specialist Pathways
    -------------------------------------------------------------------------
    , "MH_CALLSIGN_S_T" as mh_callsign_s_t
    , "MH_CALLSIGN_S_T_BASE" as mh_callsign_s_t_base
    , "MH_CALLSIGN_S_C" as mh_callsign_s_c
    , "CALLSIGN_MHTA" as callsign_mhta
    , "MH_S136" as mh_s136
    , "CALLSIGN_UCR" as callsign_ucr
    , "GREEN_TIME_S_T" as green_time_s_t
    , "H_T_CLINICALLY_CLOSED" as h_t_clinically_closed

    -------------------------------------------------------------------------
    -- Outcome
    -------------------------------------------------------------------------
    , "OUTCOME_DISPOSITION" as outcome_disposition
    , "OUTCOME_DISPOSITION_CODE" as outcome_disposition_code
    , "CONVEYANCE_GROUP_REVISED" as conveyance_group_revised

FROM {{ ref('raw_sdl_wnl_amb') }}
