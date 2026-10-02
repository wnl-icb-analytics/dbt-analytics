{{ config(materialized='table') }}

WITH

/*==============================================================================
  1. PATIENT BASE

  Structural replacement for TMP_CSDS_PAT.

  No modern reference changes have been introduced yet. The existing GP-code
  corrections and legacy Dictionary joins are retained for baseline validation.
==============================================================================*/

PATIENT_BASE AS (

    SELECT DISTINCT
        PAT."Unique_service_request_identifier",
        PAT."Pseudo_NHS_Number",
        PAT."Record_Number",

        CASE
            WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'E84706' THEN 'E84015'
            WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'E85117' THEN 'E85007'
            WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'E85110' THEN 'E85128'
            WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'E85692' THEN 'E85693'
            WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'E84029' THEN 'E84645'
            WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'E84023' THEN 'E84025'
            WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'E84690' THEN 'E84066'
            WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'E84669' THEN 'E84066'
            WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'Y02260' THEN 'E87753'
            WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'E86635' THEN 'E86618'
            WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'E84705' THEN 'E84066'
            WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'E83654' THEN 'E84012'
            WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'Y02812' THEN 'Y00352'
            WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'E84624' THEN 'E84028'
            WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'E84026' THEN 'E84006'
            WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'E84667' THEN 'E84025'
            WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'E85731' THEN 'E85051'
            WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'E85625' THEN 'E85693'
            WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'E85714' THEN 'E85694'
            WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'E84077' THEN 'E84020'
            WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'E84713' THEN 'E84069'
            WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'E85740' THEN 'E85091'
            ELSE UPPER(PAT."GP_Practice_(Latest)_DV")
        END AS GPCODE,

        PAT."Age_of_patient_at_reporting_period_start_(Years)",

        CASE
            WHEN PAT."Age_of_patient_at_reporting_period_start_(Years)" < 0
                OR PAT."Age_of_patient_at_reporting_period_start_(Years)" IS NULL
                THEN 'Unknown'
            ELSE AGEBAND."BK_AgeBand"
        END AS AGEBAND,

        AGEBAND."SK_AgeBandID",

        PAT."Ethnic_category" AS ETHNIC_CATEGORY_ORIG,
        ETH."EthnicityDesc2" AS ETHNICITYDESC_ORIG,
        PAT."Ethnic_category" AS SOURCE_ETHNICITY_CODE,
        ETH."EthnicityDesc" AS SOURCE_ETHNICITY_DESC,

        PAT."Lower_super_output_area_(Residence)",
        PAT."Person_stated_gender_code",
        PAT."Postcode_district",
        PAT."Local_authority_district_unitary_authority"

    FROM DATA_LAKE.CSDS_SIMPLE."tblPatient" PAT

    LEFT JOIN "Dictionary"."dbo"."Age" AGE
        ON PAT."Age_of_patient_at_reporting_period_start_(Years)"
            = AGE."SK_AgeID"

    LEFT JOIN "Dictionary"."dbo"."AgeBand" AGEBAND
        ON AGE."SK_AgeBandID"
            = AGEBAND."SK_AgeBandID"

    LEFT JOIN "Dictionary"."dbo"."Ethnicity" ETH
        ON PAT."Ethnic_category"
            = ETH."BK_EthnicityCode"

),

/*==============================================================================
  2. VALID PATIENT ETHNICITY

  This represents the first self-update of TMP_CSDS_PAT.

  Where a patient has another usable ethnicity recorded within CSDS, prefer it
  over a null or NOT STATED description.

  The ordering is included to ensure one deterministic row per patient.
==============================================================================*/

CSDS_PATIENT_ETHNICITY_CANDIDATES AS (

    SELECT
        PB."Pseudo_NHS_Number" AS SK_PATIENTID,
        PB.SOURCE_ETHNICITY_CODE AS ETHNICITY_CODE,
        PB.SOURCE_ETHNICITY_DESC AS ETHNICITY_DESC,

        ROW_NUMBER() OVER (
            PARTITION BY PB."Pseudo_NHS_Number"
            ORDER BY
                CASE
                    WHEN PB.SOURCE_ETHNICITY_DESC IS NOT NULL
                        AND UPPER(PB.SOURCE_ETHNICITY_DESC) <> 'NOT STATED'
                        THEN 0
                    ELSE 1
                END,
                PB."Unique_service_request_identifier",
                PB."Record_Number",
                PB.SOURCE_ETHNICITY_CODE
        ) AS ETHNICITY_ROW_NUMBER

    FROM PATIENT_BASE PB

    WHERE PB."Pseudo_NHS_Number" IS NOT NULL
      AND PB.SOURCE_ETHNICITY_DESC IS NOT NULL
      AND UPPER(PB.SOURCE_ETHNICITY_DESC) <> 'NOT STATED'

),

CSDS_PATIENT_ETHNICITY AS (

    SELECT
        SK_PATIENTID,
        ETHNICITY_CODE,
        ETHNICITY_DESC

    FROM CSDS_PATIENT_ETHNICITY_CANDIDATES

    WHERE ETHNICITY_ROW_NUMBER = 1

),

PATIENT_AFTER_CSDS_ETHNICITY AS (

    SELECT
        PB."Unique_service_request_identifier",
        PB."Pseudo_NHS_Number",
        PB."Record_Number",
        PB.GPCODE,
        PB."Age_of_patient_at_reporting_period_start_(Years)",
        PB.AGEBAND,
        PB."SK_AgeBandID",
        PB.ETHNIC_CATEGORY_ORIG,
        PB.ETHNICITYDESC_ORIG,

        CASE
            WHEN PB.SOURCE_ETHNICITY_DESC IS NULL
                OR UPPER(PB.SOURCE_ETHNICITY_DESC) = 'NOT STATED'
                THEN COALESCE(
                    CPE.ETHNICITY_CODE,
                    PB.SOURCE_ETHNICITY_CODE
                )
            ELSE PB.SOURCE_ETHNICITY_CODE
        END AS ETHNIC_CATEGORY_STAGE_1,

        CASE
            WHEN PB.SOURCE_ETHNICITY_DESC IS NULL
                OR UPPER(PB.SOURCE_ETHNICITY_DESC) = 'NOT STATED'
                THEN COALESCE(
                    CPE.ETHNICITY_DESC,
                    PB.SOURCE_ETHNICITY_DESC
                )
            ELSE PB.SOURCE_ETHNICITY_DESC
        END AS ETHNICITY_DESC_STAGE_1,

        PB."Lower_super_output_area_(Residence)",
        PB."Person_stated_gender_code",
        PB."Postcode_district",
        PB."Local_authority_district_unitary_authority"

    FROM PATIENT_BASE PB

    LEFT JOIN CSDS_PATIENT_ETHNICITY CPE
        ON PB."Pseudo_NHS_Number" = CPE.SK_PATIENTID

),

/*==============================================================================
  3. NATIONAL ETHNICITY
==============================================================================*/

NATIONAL_ETHNICITY AS (

    SELECT DISTINCT
        SK_PATIENTID,
        ETHNICITY_CODE,
        ETHNICITY_DESC,
        'NATIONAL' AS DATASET,
        1 AS SOURCE_PRIORITY

    FROM {{ ref('stg_reference_lookup_ncl_ethnicity_national_data_sets') }}

    WHERE ETHNICITY_DESC IS NOT NULL
      AND ETHNICITY_DESC NOT IN (
          'NOT STATED',
          'NOT STATED: Patient refused',
          'Not Recorded',
          'Not Stated',
          'Not stated',
          'Recorded Not Known',
          'Refused'
      )

),

/*==============================================================================
  4. OP ETHNICITY

  The existing script selects the row associated with MAX(PRIMARY_ID), rather
  than necessarily the chronologically latest event. That behaviour is retained.
==============================================================================*/

OP_ETHNICITY AS (

    SELECT DISTINCT
        A.PATIENT_ID AS SK_PATIENTID,
        A.ETHNICITY_CODE,
        A.ETHNICITY_NAME AS ETHNICITY_DESC,
        'FS_OP' AS DATASET,
        2 AS SOURCE_PRIORITY

    FROM REPORTING.MAIN_DATA.OP A

    INNER JOIN (

        SELECT
            PATIENT_ID,
            MAX(PRIMARY_ID) AS PRIMARY_ID

        FROM REPORTING.MAIN_DATA.OP

        WHERE PATIENT_ID IS NOT NULL
          AND PATIENT_ID <> 1
          AND ETHNICITY_NAME IS NOT NULL
          AND ETHNICITY_NAME NOT IN (
              'NOT STATED',
              'Unknown',
              'NOT STATED: Patient refused'
          )

        GROUP BY PATIENT_ID

    ) B
        ON A.PATIENT_ID = B.PATIENT_ID
       AND A.PRIMARY_ID = B.PRIMARY_ID

    WHERE A.PATIENT_ID IS NOT NULL
      AND A.PATIENT_ID <> 1
      AND A.ETHNICITY_NAME IS NOT NULL
      AND A.ETHNICITY_NAME NOT IN (
          'NOT STATED',
          'Unknown',
          'NOT STATED: Patient refused'
      )

),

/*==============================================================================
  5. IP ETHNICITY
==============================================================================*/

IP_ETHNICITY AS (

    SELECT DISTINCT
        A.PATIENT_ID AS SK_PATIENTID,
        A.ETHNICITY_CODE,
        A.ETHNICITY_NAME AS ETHNICITY_DESC,
        'FS_IP' AS DATASET,
        3 AS SOURCE_PRIORITY

    FROM REPORTING.MAIN_DATA.IP A

    INNER JOIN (

        SELECT
            PATIENT_ID,
            MAX(PRIMARY_ID) AS PRIMARY_ID

        FROM REPORTING.MAIN_DATA.IP

        WHERE PATIENT_ID IS NOT NULL
          AND PATIENT_ID <> 1
          AND ETHNICITY_NAME IS NOT NULL
          AND ETHNICITY_NAME NOT IN (
              'NOT STATED',
              'Unknown',
              'NOT STATED: Patient refused'
          )

        GROUP BY PATIENT_ID

    ) B
        ON A.PATIENT_ID = B.PATIENT_ID
       AND A.PRIMARY_ID = B.PRIMARY_ID

    WHERE A.PATIENT_ID IS NOT NULL
      AND A.PATIENT_ID <> 1
      AND A.ETHNICITY_NAME IS NOT NULL
      AND A.ETHNICITY_NAME NOT IN (
          'NOT STATED',
          'Unknown',
          'NOT STATED: Patient refused'
      )

),

/*==============================================================================
  6. ECDS ETHNICITY
==============================================================================*/

ECDS_ETHNICITY AS (

    SELECT DISTINCT
        A.PATIENT_ID AS SK_PATIENTID,
        A.ETHNICITY_CODE,
        A.ETHNICITY_NAME AS ETHNICITY_DESC,
        'FS_ECDS' AS DATASET,
        4 AS SOURCE_PRIORITY

    FROM REPORTING.MAIN_DATA.ECDS A

    INNER JOIN (

        SELECT
            PATIENT_ID,
            MAX(PRIMARY_ID) AS PRIMARY_ID

        FROM REPORTING.MAIN_DATA.ECDS

        WHERE PATIENT_ID IS NOT NULL
          AND PATIENT_ID <> 1
          AND ETHNICITY_NAME IS NOT NULL
          AND ETHNICITY_NAME NOT IN (
              'NOT STATED',
              'Unknown',
              'NOT STATED: Patient refused'
          )

        GROUP BY PATIENT_ID

    ) B
        ON A.PATIENT_ID = B.PATIENT_ID
       AND A.PRIMARY_ID = B.PRIMARY_ID

    WHERE A.PATIENT_ID IS NOT NULL
      AND A.PATIENT_ID <> 1
      AND A.ETHNICITY_NAME IS NOT NULL
      AND A.ETHNICITY_NAME NOT IN (
          'NOT STATED',
          'Unknown',
          'NOT STATED: Patient refused'
      )

),

/*==============================================================================
  7. FACT PROFILE ETHNICITY
==============================================================================*/

FACT_ETHNICITY AS (

    SELECT DISTINCT
        A."SK_PatientID" AS SK_PATIENTID,
        B."BK_EthnicityCode" AS ETHNICITY_CODE,
        B."EthnicityDesc" AS ETHNICITY_DESC,
        'FACT' AS DATASET,
        5 AS SOURCE_PRIORITY

    FROM DATA_LAKE.FACT_PATIENT."FactProfile" A

    INNER JOIN "Dictionary"."dbo"."Ethnicity" B
        ON A."SK_EthnicityID" = B."SK_EthnicityID"

    WHERE A."SK_DataSourceID" = 5
      AND A."PeriodEnd" = '9999-12-31 00:00:00.000'
      AND B."EthnicityDesc" IS NOT NULL
      AND B."EthnicityDesc" NOT IN (
          'NOT STATED',
          'Unknown',
          'NOT STATED: Patient refused'
      )

),

/*==============================================================================
  8. COMBINE AND PRIORITISE ETHNICITY SOURCES

  Priority reproduces the order of the original sequential INSERT statements:

      1. NATIONAL
      2. OP
      3. IP
      4. ECDS
      5. FACT
==============================================================================*/

ALL_EXTERNAL_ETHNICITY AS (

    SELECT
        SK_PATIENTID,
        ETHNICITY_CODE,
        ETHNICITY_DESC,
        DATASET,
        SOURCE_PRIORITY
    FROM NATIONAL_ETHNICITY

    UNION ALL

    SELECT
        SK_PATIENTID,
        ETHNICITY_CODE,
        ETHNICITY_DESC,
        DATASET,
        SOURCE_PRIORITY
    FROM OP_ETHNICITY

    UNION ALL

    SELECT
        SK_PATIENTID,
        ETHNICITY_CODE,
        ETHNICITY_DESC,
        DATASET,
        SOURCE_PRIORITY
    FROM IP_ETHNICITY

    UNION ALL

    SELECT
        SK_PATIENTID,
        ETHNICITY_CODE,
        ETHNICITY_DESC,
        DATASET,
        SOURCE_PRIORITY
    FROM ECDS_ETHNICITY

    UNION ALL

    SELECT
        SK_PATIENTID,
        ETHNICITY_CODE,
        ETHNICITY_DESC,
        DATASET,
        SOURCE_PRIORITY
    FROM FACT_ETHNICITY

),

EXTERNAL_ETHNICITY_PRIORITY AS (

    SELECT
        SK_PATIENTID,
        ETHNICITY_CODE,
        ETHNICITY_DESC,
        DATASET

    FROM ALL_EXTERNAL_ETHNICITY

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SK_PATIENTID
        ORDER BY
            SOURCE_PRIORITY,
            ETHNICITY_CODE,
            ETHNICITY_DESC
    ) = 1

),

/*==============================================================================
  9. APPLY EXTERNAL ETHNICITY FALLBACK
==============================================================================*/

PATIENT_ETHNICITY_ENRICHED AS (

    SELECT
        P."Unique_service_request_identifier",
        P."Pseudo_NHS_Number",
        P."Record_Number",
        P.GPCODE,
        P."Age_of_patient_at_reporting_period_start_(Years)",
        P.AGEBAND,
        P."SK_AgeBandID",
        P.ETHNIC_CATEGORY_ORIG,
        P.ETHNICITYDESC_ORIG,

        CASE
            WHEN P.ETHNICITY_DESC_STAGE_1 IS NULL
                OR UPPER(P.ETHNICITY_DESC_STAGE_1) = 'NOT STATED'
                THEN COALESCE(
                    E.ETHNICITY_CODE,
                    P.ETHNIC_CATEGORY_STAGE_1
                )
            ELSE P.ETHNIC_CATEGORY_STAGE_1
        END AS ETHNIC_CATEGORY_STAGE_2,

        CASE
            WHEN P.ETHNICITY_DESC_STAGE_1 IS NULL
                OR UPPER(P.ETHNICITY_DESC_STAGE_1) = 'NOT STATED'
                THEN COALESCE(
                    E.ETHNICITY_DESC,
                    P.ETHNICITY_DESC_STAGE_1
                )
            ELSE P.ETHNICITY_DESC_STAGE_1
        END AS ETHNICITY_DESC_STAGE_2,

        P."Lower_super_output_area_(Residence)",
        P."Person_stated_gender_code",
        P."Postcode_district",
        P."Local_authority_district_unitary_authority"

    FROM PATIENT_AFTER_CSDS_ETHNICITY P

    LEFT JOIN EXTERNAL_ETHNICITY_PRIORITY E
        ON P."Pseudo_NHS_Number" = E.SK_PATIENTID

),

/*==============================================================================
  10. CURRENT ETHNICITY CODE NORMALISATION

  Structural replacement for the last TMP_CSDS_PAT update.
==============================================================================*/

CURRENT_ETHNICITY_LOOKUP AS (

    SELECT
        "EthnicityDesc",
        MIN("BK_EthnicityCode") AS "BK_EthnicityCode"

    FROM "Dictionary"."dbo"."Ethnicity"

    WHERE "EthnicityCodeType" = 'Current'

    GROUP BY
        "EthnicityDesc"

),

PATIENT_FINAL AS (

    SELECT
        P."Unique_service_request_identifier",
        P."Pseudo_NHS_Number",
        P."Record_Number",
        P.GPCODE,
        P."Age_of_patient_at_reporting_period_start_(Years)",
        P.AGEBAND,
        P."SK_AgeBandID",
        P.ETHNIC_CATEGORY_ORIG,
        P.ETHNICITYDESC_ORIG,

        COALESCE(
            CE."BK_EthnicityCode",
            P.ETHNIC_CATEGORY_STAGE_2
        ) AS ETHNIC_CATEGORY,

        P.ETHNICITY_DESC_STAGE_2 AS "EthnicityDesc",

        P."Lower_super_output_area_(Residence)",
        P."Person_stated_gender_code",
        P."Postcode_district",
        P."Local_authority_district_unitary_authority"

    FROM PATIENT_ETHNICITY_ENRICHED P

    LEFT JOIN CURRENT_ETHNICITY_LOOKUP CE
        ON P.ETHNICITY_DESC_STAGE_2 = CE."EthnicityDesc"

),

/*==============================================================================
  11. FIRST ATTENDED CARE CONTACT

  Structural replacement for TMP_CON.
==============================================================================*/

FIRST_ATTENDED_CONTACT_DATE AS (

    SELECT
        "Unique_service_request_identifier",
        MIN("Care_contact_date") AS FIRST_CARE_CONTACT_DATE

    FROM DATA_LAKE.CSDS_SIMPLE."tblCare_Contact"

    WHERE "Attended_or_did_not_attend_code" IN ('5', '6')

    GROUP BY
        "Unique_service_request_identifier"

),

/*==============================================================================
  12. IMD LOOKUP

  Structural replacement for TMP_IMD.
==============================================================================*/

IMD_LOOKUP AS (

    SELECT *

    FROM DATA_LAKE__NCL.ANALYST_MANAGED.IMD_2025

    WHERE REGEXP_LIKE(
        LOCAL_AUTHORITY_DISTRICT_NAME_2024,
        'Barnet|Enfield|Camden|Islington|Haringey|Brent|Harrow|Hillingdon|Central London|West London|Hammersmith and Fulham|Hounslow|Ealing|Westminster|Kensington and Chelsea',
        'i'
    )

),

/*==============================================================================
  13. CONTACT ENRICHMENT

  Current legacy joins are retained for baseline validation. They can be
  replaced individually by the proven Referrals REFERENCE/UKHFD hierarchy
  after the structural conversion has been validated.
==============================================================================*/

NATIONAL_GP AS (

    SELECT
        GP_PRACTICE_CODE,
        GP_PRACTICE_NAME

    FROM {{ ref('raw_reference_national_gp_practice_latest_list_sizes') }}

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY GP_PRACTICE_CODE
        ORDER BY
            LIST_SIZE_DATE DESC,
            LAST_UPDATED DESC
    ) = 1

),

CONTACT_ENRICHED AS (

    SELECT
        CCON.*,

        PAT1."Record_Number" AS PAT_RECORD_NUMBER,
        PAT1.GPCODE,
        PAT1."Age_of_patient_at_reporting_period_start_(Years)" AS PATIENT_AGE,
        PAT1."SK_AgeBandID" AS PATIENT_AGE_BAND_CODE,
        PAT1.AGEBAND AS PATIENT_AGE_BAND_DESC,
        PAT1.ETHNIC_CATEGORY,
        PAT1."EthnicityDesc",
        PAT1.ETHNIC_CATEGORY_ORIG,
        PAT1.ETHNICITYDESC_ORIG,
        PAT1."Person_stated_gender_code" AS PATIENT_GENDER_CODE,
        PAT1."Lower_super_output_area_(Residence)" AS PATIENT_LSOA,
        PAT1."Postcode_district" AS PATIENT_POSTCODE_DISTRICT,
        PAT1."Local_authority_district_unitary_authority" AS PATIENT_LA_CODE,

        ORG1."Organisation_Name" AS LEGACY_PROVIDER_NAME,
        ORG3."Organisation_Name" AS LEGACY_PROVIDER_SITE_NAME,
        ORG2."Organisation_Name" AS LEGACY_COMMISSIONER_NAME,

        PRAC.BOROUGH AS LEGACY_PRACTICE_BOROUGH,
        PRAC.PCN_CODE AS LEGACY_PCN_CODE,
        PRAC.PCN_NAME AS LEGACY_PCN_NAME,
        PRAC.PRACTICE_NAME AS LEGACY_PRACTICE_NAME,
        NAT_GP.GP_PRACTICE_NAME AS NATIONAL_PRACTICE_NAME,

        PWP.WEIGHTED_PATIENTS_CORE AS LEGACY_WEIGHTED_LIST_SIZE,
        
        GEN."Gender" AS LEGACY_GENDER_NAME,

        IMD.INDEX_OF_MULTIPLE_DEPRIVATION_IMD_DECILE_WHERE_1_IS_MOST_DEPRIVED_10_PERCENT__OF_LSOAS
            AS IMD_DECILE,

        IMD.TOTAL_POPULATION_MID_2022 AS IMD_TOTAL_POPULATION,

        CSLK1.DESCRIPTION AS ATTENDED_LOOKUP_DESCRIPTION,
        CSLK3.DESCRIPTION AS CONSULTATION_MEDIUM_DESCRIPTION,
        CSLK4.DESCRIPTION AS ACTIVITY_LOCATION_DESCRIPTION,
        CSLK5.DESCRIPTION AS SERVICE_TYPE_DESCRIPTION,
        CSLK6.DESCRIPTION AS GROUP_THERAPY_DESCRIPTION,

        REF."ServOrTeamTypeRefToCC_(Latest_List)_DV"
            AS REFERRAL_SERVICE_TYPE_CODE,

        FACD.FIRST_CARE_CONTACT_DATE,

        NGH_RES.NEIGHBOURHOOD_NAME AS RESIDENCE_NEIGHBOURHOOD_NAME,
        PRAC.NEIGHBOURHOOD_NAME AS PRACTICE_NEIGHBOURHOOD_NAME

    FROM DATA_LAKE.CSDS_SIMPLE."tblCare_Contact" CCON

    LEFT JOIN PATIENT_FINAL PAT1
        ON CCON."Unique_service_request_identifier"
            = PAT1."Unique_service_request_identifier"

    LEFT JOIN "Dictionary"."dbo"."Organisation" ORG1
        ON CCON."Organisation_identifier_(Code_of_provider)"
            = ORG1."Organisation_Code"

    LEFT JOIN "Dictionary"."dbo"."Organisation" ORG3
        ON CCON."Site_code_(of_treatment)"
            = ORG3."Organisation_Code"

    LEFT JOIN "Dictionary"."dbo"."Organisation" ORG2
        ON CCON."Organisation_identifier_(Code_of_commissioner)"
            = ORG2."Organisation_Code"

    LEFT JOIN "Dictionary"."dbo"."Gender" GEN
        ON PAT1."Person_stated_gender_code"
            = GEN."GenderCode"

    LEFT JOIN IMD_LOOKUP IMD
        ON PAT1."Lower_super_output_area_(Residence)"
            = IMD.LSOA_CODE_2021

    LEFT JOIN {{ ref('attendance_status') }} CSLK1
        ON CCON."Attended_or_did_not_attend_code" = CSLK1.CODE
       AND CSLK1.SOURCE_CODE_SET_NAME
            = 'Attended_or_did_not_attend_code'

    LEFT JOIN {{ ref('consultation_mechanism') }} CSLK3
        ON CCON."Consultation_medium_used" = CSLK3.CODE
       AND CSLK3.SOURCE_CODE_SET_NAME
            = 'Consultation_medium_used'

    LEFT JOIN {{ ref('activity_location_type') }} CSLK4
        ON CCON."Activity_location_type_code" = CSLK4.CODE
       AND CSLK4.SOURCE_CODE_SET_NAME
            = 'Activity_location_type_code'

    LEFT JOIN {{ ref('stg_reference_lookup_ncl_gp_practice') }} PRAC
        ON PAT1.GPCODE = PRAC.GP_PRACTICE_CODE        

    LEFT JOIN DATA_LAKE.CSDS_SIMPLE."tblReferral" REF
        ON CCON."Unique_service_request_identifier"
            = REF."Unique_service_request_identifier"
       AND CCON."Unique_service_request_identifier" IS NOT NULL

    LEFT JOIN {{ ref('csds_service_or_team_type') }} CSLK5
        ON LPAD(LTRIM(LEFT(REF."ServOrTeamTypeRefToCC_(Latest_List)_DV",2),'0#'),2,'0') 
            = LPAD(CSLK5.CODE, 2, '0')

    LEFT JOIN DATA_LAKE__NCL.ANALYST_MANAGED.CSDS_LOOKUP CSLK6
        ON CCON."Group_therapy_indicator" = CSLK6.CODE
       AND CSLK6.CSDS_FIELDNAME = 'GROUP THERAPY INDICATOR'

    LEFT JOIN FIRST_ATTENDED_CONTACT_DATE FACD
        ON CCON."Unique_service_request_identifier"
            = FACD."Unique_service_request_identifier"

    LEFT JOIN {{ ref('practice_weighted_population_current') }} PWP
        ON PAT1.GPCODE = PWP.PRACTICE_CODE

    LEFT JOIN NATIONAL_GP NAT_GP
        ON PAT1.GPCODE = NAT_GP.GP_PRACTICE_CODE

    LEFT JOIN DATA_LAKE__NCL.ANALYST_MANAGED.NCL_NEIGHBOURHOOD_LSOA_2021 NGH_RES
        ON PAT1."Lower_super_output_area_(Residence)"
            = NGH_RES.LSOA_2021_CODE

    WHERE CCON."Care_contact_date" >= '2021-04-01'

),

/*==============================================================================
  14. CALCULATE SERVICE TYPE ONCE

  This avoids maintaining two partially duplicated CASE statements for the
  service code and name.
==============================================================================*/



CONTACT_SERVICE_TYPE AS (

    SELECT
        CE.*,

        SUBSTRING(
            CE."Unique_care_professional_team_local_identifier",
            4,
            50
        ) AS TEAM_LOCAL_ID_SUFFIX,

        CASE
            WHEN LEFT(CE.REFERRAL_SERVICE_TYPE_CODE, 2) = '22'
                THEN '58'

            WHEN LEFT(CE.REFERRAL_SERVICE_TYPE_CODE, 2) = '28'
                THEN '57'

            WHEN LEFT(CE.REFERRAL_SERVICE_TYPE_CODE, 1) = '4'
                THEN '04'

            WHEN CE."Organisation_identifier_(Code_of_provider)" = 'RKE'
             AND (
                    CE.REFERRAL_SERVICE_TYPE_CODE IS NULL
                    OR CE.REFERRAL_SERVICE_TYPE_CODE = '99'
                 )
             AND UPPER(
                    SUBSTRING(
                        CE."Unique_care_professional_team_local_identifier",
                        4,
                        50
                    )
                 ) IN (
                    'HFHEA',
                    'HFHWE',
                    'HFINO',
                    'HFISO',
                    'HFPMHG',
                    'HFPO',
                    'HFPSG'
                 )
                THEN '04'

            WHEN CE."Organisation_identifier_(Code_of_provider)" = 'RKE'
             AND (
                    CE.REFERRAL_SERVICE_TYPE_CODE IS NULL
                    OR CE.REFERRAL_SERVICE_TYPE_CODE = '99'
                 )
             AND UPPER(
                    SUBSTRING(
                        CE."Unique_care_professional_team_local_identifier",
                        4,
                        50
                    )
                 ) IN (
                    'ADNC',
                    'ADNCE',
                    'ADNNE',
                    'ADNT',
                    'ADNW',
                    'DNIOT'
                 )
                THEN '12'

            WHEN CE."Organisation_identifier_(Code_of_provider)" = 'RKE'
             AND (
                    CE.REFERRAL_SERVICE_TYPE_CODE IS NULL
                    OR CE.REFERRAL_SERVICE_TYPE_CODE = '99'
                 )
             AND UPPER(
                    SUBSTRING(
                        CE."Unique_care_professional_team_local_identifier",
                        4,
                        50
                    )
                 ) IN (
                    'OTBPW',
                    'OTBULC'
                 )
                THEN '23'

            WHEN CE."Organisation_identifier_(Code_of_provider)" = 'RKE'
             AND (
                    CE.REFERRAL_SERVICE_TYPE_CODE IS NULL
                    OR CE.REFERRAL_SERVICE_TYPE_CODE = '99'
                 )
             AND UPPER(
                    SUBSTRING(
                        CE."Unique_care_professional_team_local_identifier",
                        4,
                        50
                    )
                 ) IN (
                    'PBACUTE',
                    'PHYBEAPEA',
                    'PHYBLTC',
                    'PHYBPSAC',
                    'PHYBT'
                 )
                THEN '26'

            WHEN CE."Organisation_identifier_(Code_of_provider)" = 'RKE'
             AND (
                    CE.REFERRAL_SERVICE_TYPE_CODE IS NULL
                    OR CE.REFERRAL_SERVICE_TYPE_CODE = '99'
                 )
             AND UPPER(
                    SUBSTRING(
                        CE."Unique_care_professional_team_local_identifier",
                        4,
                        50
                    )
                 ) IN (
                    'BANDSSLTPU',
                    'SLTBCND',
                    'SLTBEHCPR',
                    'SLTBEYLSS',
                    'SLTBEYPWSC',
                    'SLTBIASVD',
                    'SLTBIAXSC',
                    'SLTBPSL',
                    'SLTBSGISC',
                    'SLTBSS'
                 )
                THEN '33'

            WHEN CE."Organisation_identifier_(Code_of_provider)" = 'RKE'
             AND (
                    CE.REFERRAL_SERVICE_TYPE_CODE IS NULL
                    OR CE.REFERRAL_SERVICE_TYPE_CODE = '99'
                 )
             AND UPPER(
                    SUBSTRING(
                        CE."Unique_care_professional_team_local_identifier",
                        4,
                        50
                    )
                 ) IN (
                    'CF-W-AC3IR',
                    'CF-W-AC3HR',
                    'CF-W-AC3RO',
                    'CF-W-AC3RR'
                 )
                THEN '51'

            WHEN CE."Organisation_identifier_(Code_of_provider)" = 'RKE'
             AND UPPER(
                    SUBSTRING(
                        CE."Unique_care_professional_team_local_identifier",
                        4,
                        50
                    )
                 ) = 'PV'
                THEN '45'

            ELSE CE.REFERRAL_SERVICE_TYPE_CODE
        END AS DERIVED_SERVICE_TYPE_CODE

    FROM CONTACT_ENRICHED CE

),


/*==============================================================================
  15. FINAL OUTPUT
==============================================================================*/

FINAL AS (

    SELECT
        CURRENT_DATE() AS REFRESH_DATE,

        PAT_RECORD_NUMBER AS RECORD_NUMBER,

        "Unique_care_contact_identifier" AS PRIMARY_ID,
        "Unique_service_request_identifier" AS UNIQUE_ID,
        "NHSNumber_Pseudo" AS PATIENT_ID,
        "Person_ID" AS PERSON_ID,

        'CSDS-Simple-Care Contact' AS DATASET,
        'Care Contact' AS POD_GROUP,

        CASE
            WHEN "Consultation_type" = '01'
                THEN 'First Consultation'
            WHEN "Consultation_type" = '02'
                THEN 'Follow Up Consultation'
            ELSE 'Unknown/Invalid'
        END AS POD,

        REPORTING.MAIN_DATA.DETERMINE_FISCAL_YEAR__CH_TEMP(
            "Care_contact_date"
        ) AS FIN_YEAR,

        MONTHNAME("Care_contact_date") AS FIN_MONTH_TEXT,

        REPORTING.MAIN_DATA.DETERMINE_FISCAL_MONTH__CH_TEMP(
            "Care_contact_date"
        ) AS FIN_MONTH,

        "Care_contact_date"::DATE AS START_DATE,
        "Care_contact_time"::TIME AS START_TIME,

        NULL::DATE AS END_DATE,

        LAST_DAY("Care_contact_date", 'WEEK') AS WEEKEND_DATE,

        "Organisation_identifier_(Code_of_provider)" AS PROVIDER_CODE,
        LEGACY_PROVIDER_NAME AS PROVIDER_NAME,

        "Site_code_(of_treatment)" AS PROVIDER_SITE_CODE,
        LEGACY_PROVIDER_SITE_NAME AS PROVIDER_SITE_NAME,

        "Organisation_identifier_(Code_of_commissioner)"
            AS COMMISSIONER_CODE,

        LEGACY_COMMISSIONER_NAME AS COMMISSIONER_NAME,

        COALESCE(
            LEGACY_PRACTICE_BOROUGH,
            'Non-NCL/Unknown/Invalid'
        ) AS BOROUGH,

        CASE
            WHEN LEGACY_PCN_CODE IS NULL
                THEN 'Non-NCL/Unknown/Invalid'
            ELSE LEGACY_PCN_NAME
        END AS PCN,

        COALESCE(
            LEGACY_PCN_CODE,
            'Non-NCL/Unknown/Invalid'
        ) AS PCN_CODE,

        GPCODE AS GP_PRACTICE_CODE,

        COALESCE(
            LEGACY_PRACTICE_NAME,
            NATIONAL_PRACTICE_NAME,
            'Non-NCL/Unknown/Invalid'
        ) AS GP_PRACTICE_NAME,

        CASE
            WHEN PATIENT_LA_CODE = 'E09000003' THEN 'Barnet'
            WHEN PATIENT_LA_CODE = 'E09000007' THEN 'Camden'
            WHEN PATIENT_LA_CODE = 'E09000010' THEN 'Enfield'
            WHEN PATIENT_LA_CODE = 'E09000014' THEN 'Haringey'
            WHEN PATIENT_LA_CODE = 'E09000019' THEN 'Islington'
            WHEN PATIENT_LA_CODE = 'E09000005' THEN 'Brent'
            WHEN PATIENT_LA_CODE = 'E09000009' THEN 'Ealing'
            WHEN PATIENT_LA_CODE = 'E09000020' THEN 'West London'
            WHEN PATIENT_LA_CODE = 'E09000015' THEN 'Harrow'
            WHEN PATIENT_LA_CODE = 'E09000018' THEN 'Hounslow'
            WHEN PATIENT_LA_CODE = 'E09000017' THEN 'Hillingdon'
            WHEN PATIENT_LA_CODE = 'E09000033' THEN 'Central London'
            WHEN PATIENT_LA_CODE = 'E09000013'
                THEN 'Hammersmith and Fulham'
            ELSE 'Non-WNL residence'
        END AS BOROUGH_OF_RESIDENCE,

        LEGACY_WEIGHTED_LIST_SIZE AS LIST_SIZE,

        PATIENT_AGE AS AGE,
        PATIENT_AGE_BAND_CODE AS AGE_BAND_CODE,
        PATIENT_AGE_BAND_DESC AS AGE_BAND_DESC,

        CASE
            WHEN PATIENT_AGE BETWEEN 0 AND 17 THEN '0-17'
            WHEN PATIENT_AGE BETWEEN 18 AND 64 THEN '18-64'
            WHEN PATIENT_AGE >= 65 THEN '65+'
        END AS AGE_GROUP_DESC,

        ETHNIC_CATEGORY AS ETHNICITY_CODE,
        "EthnicityDesc" AS ETHNICITY_NAME,
        ETHNIC_CATEGORY_ORIG AS ETHNIC_CATEGORY_CODE,
        ETHNICITYDESC_ORIG AS ETHNIC_DESC_ORIG,

        CASE
            WHEN "EthnicityDesc" IS NULL
                THEN 'Unknown'
            ELSE INITCAP(
                SPLIT_PART("EthnicityDesc", ':', 1)
            )
        END AS ETHNICITY_GROUPING,

        CASE
            WHEN "EthnicityDesc" IS NULL
                THEN 'Not Stated/Unknown/Invalid'

            WHEN "EthnicityDesc" IN (
                'Not stated',
                'Not Known'
            )
                THEN 'Not Stated/Unknown/Invalid'

            ELSE ETHNICITYDESC_ORIG
        END AS ETHNICITY_SUB_GROUPING,

        PATIENT_GENDER_CODE AS GENDER_CODE,
        LEGACY_GENDER_NAME AS GENDER_NAME,

        PATIENT_LSOA AS LSOA,
        IMD_DECILE AS DEPRIVATION_DECILE,

        CASE
            WHEN IMD_DECILE IN (1, 2)
                THEN 'Most Deprived'
            WHEN IMD_DECILE IN (3, 4)
                THEN 'Second Most Deprived'
            WHEN IMD_DECILE IN (5, 6)
                THEN 'Third Most Deprived'
            WHEN IMD_DECILE IN (7, 8)
                THEN 'Second Least Deprived'
            WHEN IMD_DECILE IN (9, 10)
                THEN 'Least Deprived'
            WHEN IMD_DECILE IS NULL OR IMD_DECILE = 0
                THEN 'Unknown'
        END AS PATIENT_IMD_QUINTILE_25,

        CASE
            WHEN IMD_DECILE IN (1, 2) THEN 5
            WHEN IMD_DECILE IN (3, 4) THEN 4
            WHEN IMD_DECILE IN (5, 6) THEN 3
            WHEN IMD_DECILE IN (7, 8) THEN 2
            WHEN IMD_DECILE IN (9, 10) THEN 1
            WHEN IMD_DECILE IS NULL OR IMD_DECILE = 0 THEN 6
        END AS PATIENT_IMD_QUINTILE_25_ORDER,

        IMD_TOTAL_POPULATION AS POP_BY_DEPRIVATION,

        PATIENT_POSTCODE_DISTRICT AS PATIENT_POSTOCDE_DISTRICT,

        "Attended_or_did_not_attend_code" AS ATTENDED_CODE,
        ATTENDED_LOOKUP_DESCRIPTION AS ATTENDED_NAME,

        "Consultation_medium_used"
            AS CONSULTATION_MECHANISM_USED_CODE,

        CASE
            WHEN "Consultation_medium_used" IN ('1', '01')
                THEN 'Face to face'
            ELSE 'Non-face to face'
        END AS CONSULTATION_MECHANISM_GROUP,

        CASE
            WHEN "Consultation_medium_used" = '4'
                THEN 'Talk type for a person unable to speak'
            ELSE CONSULTATION_MEDIUM_DESCRIPTION
        END AS CONSULTATION_MECHANISM_USED_NAME,

        "Activity_location_type_code"
            AS ACTIVITY_LOCATION_TYPE_CODE,

        CASE
            WHEN "Consultation_medium_used" NOT IN ('1', '01')
                THEN 'Not applicable'
            ELSE ACTIVITY_LOCATION_DESCRIPTION
        END AS ACTIVITY_LOCATION_TYPE_NAME,

        DERIVED_SERVICE_TYPE_CODE
            AS SERVICE_OR_TEAM_TYPE_REFERRED_TO_CODE,

        CASE
            WHEN DERIVED_SERVICE_TYPE_CODE = '58'
                THEN 'Nutrition and Dietetics Service (Excluding Weight Management)'

            WHEN DERIVED_SERVICE_TYPE_CODE = '57'
                THEN 'Public Health and Lifestyle Service (Excluding Weight Management)'

            WHEN DERIVED_SERVICE_TYPE_CODE = '04'
                THEN 'Cardiac Service'

            WHEN DERIVED_SERVICE_TYPE_CODE = '12'
                THEN 'District Nursing Service'

            WHEN DERIVED_SERVICE_TYPE_CODE = '23'
                THEN 'Occupational Therapy Service'

            WHEN DERIVED_SERVICE_TYPE_CODE = '26'
                THEN 'Physiotherapy Service'

            WHEN DERIVED_SERVICE_TYPE_CODE = '33'
                THEN 'Speech and Language Therapy Service'

            WHEN DERIVED_SERVICE_TYPE_CODE = '51'
                THEN 'Crisis Response Intermediate Care Service'

            WHEN DERIVED_SERVICE_TYPE_CODE = '45'
                THEN 'Integrated Multidisciplinary Team (jointly commissioned)'

            ELSE SERVICE_TYPE_DESCRIPTION
        END AS SERVICE_OR_TEAM_TYPE_REFERRED_TO_NAME,

        "Clinical_contact_duration_care_contact"
            AS CLINICAL_CONTACT_DURATION_CARE_CONTACT,

        "Time_between_referral_and_care_contact"
            AS RESPONSE_TIME_DAYS,

        "Time_between_referral_and_care_contact" / 7
            AS RESPONSE_TIME_WEEKS,

        CASE
            WHEN "Care_contact_date" = FIRST_CARE_CONTACT_DATE
                THEN 'Y'
            ELSE 'N'
        END AS FIRST_CARE_CONTACT,

        CASE
            WHEN LEFT(
                    "Organisation_identifier_(Code_of_provider)",
                    3
                 ) = 'RYX'
             AND TEAM_LOCAL_ID_SUFFIX IN (
                    '2_157',
                    '2_310',
                    '2_459',
                    '2_553',
                    '2_711',
                    '2_764'
                 )
                THEN 'TRUE'

            WHEN LEFT(
                    "Organisation_identifier_(Code_of_provider)",
                    3
                 ) = 'RV3'
             AND TEAM_LOCAL_ID_SUFFIX IN (
                    '592363_556095651102',
                    '599565_556095651102',
                    '705299_556095651102',
                    '717245_556095651102',
                    '717246_556095651102',
                    '717266_556095651102',
                    '717267_556095651102'
                 )
                THEN 'TRUE'

            WHEN LEFT(
                    "Organisation_identifier_(Code_of_provider)",
                    3
                 ) = 'RKE'
             AND TEAM_LOCAL_ID_SUFFIX IN (
                    'PCSTE',
                    'PCSW',
                    'PCSWE',
                    'PCSNR',
                    'PCST',
                    'PCSASESS',
                    'PCSFU',
                    'PCSFUE'
                 )
                THEN 'TRUE'

            WHEN LEFT(
                    "Organisation_identifier_(Code_of_provider)",
                    3
                 ) IN ('RRP', 'RAP', 'RAL')
             AND TEAM_LOCAL_ID_SUFFIX = 'MEPCST'
                THEN 'TRUE'

            ELSE 'FALSE'
        END AS IS_POST_COVID,

        "Unique_care_professional_team_local_identifier"
            AS UNIQUE_CARE_PROFESSIONAL_TEAM_LOCAL_ID,

        COALESCE(
            RESIDENCE_NEIGHBOURHOOD_NAME,
            'Unknown'
        ) AS NEIGHBOURHOOD_OF_RESIDENCE,

        COALESCE(
            PRACTICE_NEIGHBOURHOOD_NAME,
            'Unknown'
        ) AS PRACTICE_NEIGHBOURHOOD,

        "Group_therapy_indicator"
            AS GROUP_THERAPY_INDICATOR_CODE,

        COALESCE(
            GROUP_THERAPY_DESCRIPTION,
            'Not known'
        ) AS GROUP_THERAPY_INDICATOR_DESC,

        1 AS ACTIVITY

    FROM CONTACT_SERVICE_TYPE

)

SELECT *
FROM FINAL
