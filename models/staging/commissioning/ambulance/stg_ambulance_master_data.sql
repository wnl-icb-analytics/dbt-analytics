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
      DLP_FLEX_OR_FREEZE                 
    , DLP_COMMISSIONER_CODE             
    , DLP_FINANCIAL_MONTH                
    , DLP_FINANCIAL_YEAR        
    , DLP_BASELINE_FINANCIAL_MONTH

    -------------------------------------------------------------------------
    -- Incident Identifiers
    -------------------------------------------------------------------------
    , INCIDENT_ID
    , CAD_NUMBER

    -------------------------------------------------------------------------
    -- Dates and Times
    -------------------------------------------------------------------------
    , DATE_VALUE
    , CALL_TIME
    , CLOCK_START_TIME
    , ARRIVE_ON_SCENE_FIRST_VEHICLE
    , ARRIVE_ON_SCENE_FIRST_TRANSPORTING_VEHICLE
    , CONVEYING_DATETIME
    , PRE_TRIAGED_QUESTIONS_TIME

    -------------------------------------------------------------------------
    -- Geography & Organisation
    -------------------------------------------------------------------------
    , LOCALITY
    , ICB
    , CROSS_BORDER_FLAG
    , DV_LSOA
    , DV_PARTIAL_POST_CODE
    , DV_PARTIAL_POST_CODE_DESTINATION_POSTCODE
    , DV_RECIPIENT_CODE
    , NHS_SITECODE
    , GP_LOCATION

    -------------------------------------------------------------------------
    -- Patient Demographics
    -------------------------------------------------------------------------
    , CALL_AGE
    , CALL_GENDER
    , SK_PATIENT_ID_NHS_NUMBER
    , CARE_HOME_LOCATION_FLAG

    -------------------------------------------------------------------------
    -- Clinical Information
    -------------------------------------------------------------------------
    , DETERMINANT_CODE
    , CHIEF_COMPLAINT_CODE
    , AMPDS_CODE_VERSION
    , INCIDENT_CODE
    , ILLNESS_CODE_1
    , ILLNESS_CODE_2
    , ILLNESS_INCIDENT_CODE_VERSION
    , DO_H_CATEGORY
    , FULL_DOH_CATEGORY
    , SUB_CATEGORY
    , CALL_HRG_CODE
    , INCIDENT_HRG_CODE
    , HRG_VERSION_NUMBER
    , PRE_TRIAGED_QUESTIONS
    , ADDITIONAL_H_T

    -------------------------------------------------------------------------
    -- Operational Information
    -------------------------------------------------------------------------
    , CALL_TYPE
    , DESTINATION_REFERRAL_OUTCOME
    , MODE_OF_TRANSPORT_FLAG
    , MODE_OF_TRANSPORT_FIRST_RESPONDER
    , CONVEYING_CREW_TYPE
    , FIRST_RESPONDER_CREW_TYPE
    , DEPARTMENT_TYPE_FLAG
    , PRE_ALERT_BLUE_CALL_FLAG
    , H_T_TAXI_FLAG
    , BASELINE_SETTING_INCLUSIONS

    -------------------------------------------------------------------------
    -- Mental Health / Specialist Pathways
    -------------------------------------------------------------------------
    , MH_CALLSIGN_S_T
    , MH_CALLSIGN_S_T_BASE
    , MH_CALLSIGN_S_C
    , CALLSIGN_MHTA
    , MH_S136
    , CALLSIGN_UCR
    , GREEN_TIME_S_T
    , H_T_CLINICALLY_CLOSED

    -------------------------------------------------------------------------
    -- Outcome
    -------------------------------------------------------------------------
    , OUTCOME_DISPOSITION
    , OUTCOME_DISPOSITION_CODE
    , CONVEYANCE_GROUP_REVISED

FROM {{ ref('raw_sdl_wnl_amb') }}
