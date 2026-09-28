{{ config(materialized='table') }}

WITH 
---------------------------------------------------------------------------------------------------------
-- 0. OVERRIDE LOOKUPS & UNIFIED COMMISSIONER CTE
---------------------------------------------------------------------------------------------------------
MISSING_CODES_LOOKUP AS (
    SELECT 'T1AA' AS CODE, 'Specsavers Optical Group' AS NAME
    UNION ALL
    SELECT 'TAD', 'Bradford District Care NHS Foundation Trust'
    UNION ALL
    SELECT 'TAJ', 'Black Country Healthcare NHS Foundation Trust'
    UNION ALL
    SELECT 'TAH', 'Sheffield Health Partnership University NHS Foundation Trust'
),

-- Multi-source Commissioner mapping CTE
COMMISSIONER_LOOKUP AS (

    SELECT
        UPPER(TRIM(CODE)) AS CODE,
        NAME
    FROM (

        SELECT
            Commissioner_Code AS CODE,
            Commissioner_Name AS NAME,
            1 AS PRIORITY
        from {{ ref('stg_dictionary_dbo_commissioner') }}

        UNION ALL

        SELECT
            "Organisation_Code" AS CODE,
            "Organisation_Name" AS NAME,
            2 AS PRIORITY
        FROM "Dictionary"."dbo"."Organisation"

    )

    WHERE CODE IS NOT NULL
      AND TRIM(CODE) <> ''

    QUALIFY ROW_NUMBER()
        OVER (
            PARTITION BY UPPER(TRIM(CODE))
            ORDER BY PRIORITY, NAME
        ) = 1

),

NORMALISED_REFERRALS AS (
    SELECT 
        REF.*,
        LPAD(
            TRIM(
                SPLIT_PART(
                    CASE 
                        WHEN LEFT(TRIM(REF."ServOrTeamTypeRefToCC_(Latest_List)_DV"), 2) = '22' THEN '58'
                        WHEN LEFT(TRIM(REF."ServOrTeamTypeRefToCC_(Latest_List)_DV"), 2) = '28' THEN '57'
                        WHEN LEFT(TRIM(REF."ServOrTeamTypeRefToCC_(Latest_List)_DV"), 2) = '4'  THEN '04'
                        WHEN LEFT(TRIM(REF."ServOrTeamTypeRefToCC_(Latest_List)_DV"), 2) = '99' 
                             AND LEFT(REF."Organisation_identifier_(Code_of_provider)", 3) = 'RKE' THEN '12'
                        ELSE REF."ServOrTeamTypeRefToCC_(Latest_List)_DV" 
                    END,
                    ',', 1
                )
            ),
            2, '0'
        ) AS CLEAN_TEAM_CODE
    FROM DATA_LAKE.CSDS_SIMPLE."tblReferral" REF
),

REFERRING_ORGANISATION_LOOKUP AS (
    SELECT CODE, NAME FROM (
        SELECT 
            UPPER(TRIM(CODE)) AS CODE, 
            NAME, 
            PRIORITY
        FROM (
            -- Priority 1: Modern Reference Schema (Providers, Sites & Local Authorities)
            SELECT ORGANISATION_CODE AS CODE, ORGANISATION_NAME AS NAME, 1 AS PRIORITY 
            FROM REFERENCE.ORGANISATION.ORGANISATION_NHS_PROVIDER            
            UNION ALL            
            SELECT ORGANISATION_CODE AS CODE, ORGANISATION_NAME AS NAME, 1 AS PRIORITY 
            FROM REFERENCE.ORGANISATION.ORGANISATION_NHS_SITE            
            UNION ALL            
            SELECT ORGANISATION_CODE AS CODE, ORGANISATION_NAME AS NAME, 1 AS PRIORITY 
            FROM REFERENCE.ORGANISATION.ORGANISATION_LOCAL_AUTHORITY
            UNION ALL            
            -- Priority 2: Legacy Dictionary Fallback (Exact Code Match Only)
            SELECT "Organisation_Code" AS CODE, "Organisation_Name" AS NAME, 2 AS PRIORITY 
            FROM "Dictionary"."dbo"."Organisation"
        )
        WHERE CODE IS NOT NULL AND CODE <> ''
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY PRIORITY ASC, NAME ASC) = 1
),

PATIENT_DEDUPED AS (
    SELECT * 
    FROM (
        SELECT 
            PAT.*,
            ROW_NUMBER() OVER (
                PARTITION BY PAT."Unique_service_request_identifier" 
                ORDER BY PAT."Record_Number" DESC
            ) AS RN
        FROM DATA_LAKE.CSDS_SIMPLE."tblPatient" PAT
    )
    WHERE RN = 1
),

--------------------------------------------------------------------------------
-- 1. DEDUPLICATED ETHNICITY LOOKUP (Priority Waterfall via ROW_NUMBER)
--------------------------------------------------------------------------------
ALL_ETHNICITY_SOURCES AS (
    SELECT 
        SK_PATIENTID AS PATIENT_ID, 
        ETHNICITY_CODE, 
        ETHNICITY_DESC, 
        1 AS PRIORITY_RANK
    FROM DEV__MODELLING.LOOKUP_NCL.ETHNICITY_NATIONAL_DATA_SETS
    WHERE ETHNICITY_DESC NOT IN ('NOT STATED','NOT STATED: Patient refused','Not Recorded','Not Stated','Not stated','Recorded Not Known','Refused') 
      AND ETHNICITY_DESC IS NOT NULL

    UNION ALL

    SELECT 
        A.PATIENT_ID, 
        A.ETHNICITY_CODE, 
        A.ETHNICITY_NAME AS ETHNICITY_DESC, 
        2 AS PRIORITY_RANK
    FROM REPORTING.MAIN_DATA.OP A
    INNER JOIN (
        SELECT PATIENT_ID, MAX(PRIMARY_ID) AS PRIMARY_ID
        FROM REPORTING.MAIN_DATA.OP
        WHERE PATIENT_ID IS NOT NULL 
          AND PATIENT_ID <> 1 
          AND ETHNICITY_NAME NOT IN ('NOT STATED','Unknown','NOT STATED: Patient refused') 
          AND ETHNICITY_NAME IS NOT NULL 
        GROUP BY PATIENT_ID
    ) B ON A.PATIENT_ID = B.PATIENT_ID AND A.PRIMARY_ID = B.PRIMARY_ID

    UNION ALL

    SELECT 
        A.PATIENT_ID, 
        A.ETHNICITY_CODE, 
        A.ETHNICITY_NAME AS ETHNICITY_DESC, 
        3 AS PRIORITY_RANK
    FROM REPORTING.MAIN_DATA.IP A
    INNER JOIN (
        SELECT PATIENT_ID, MAX(PRIMARY_ID) AS PRIMARY_ID
        FROM REPORTING.MAIN_DATA.IP 
        WHERE PATIENT_ID IS NOT NULL 
          AND PATIENT_ID <> 1 
          AND ETHNICITY_NAME NOT IN ('NOT STATED','Unknown','NOT STATED: Patient refused') 
          AND ETHNICITY_NAME IS NOT NULL 
        GROUP BY PATIENT_ID
    ) B ON A.PATIENT_ID = B.PATIENT_ID AND A.PRIMARY_ID = B.PRIMARY_ID

    UNION ALL

    SELECT 
        A.PATIENT_ID, 
        A.ETHNICITY_CODE, 
        A.ETHNICITY_NAME AS ETHNICITY_DESC, 
        4 AS PRIORITY_RANK
    FROM REPORTING.MAIN_DATA.ECDS A
    INNER JOIN (
        SELECT PATIENT_ID, MAX(PRIMARY_ID) AS PRIMARY_ID
        FROM REPORTING.MAIN_DATA.ECDS 
        WHERE PATIENT_ID IS NOT NULL 
          AND PATIENT_ID <> 1 
          AND ETHNICITY_NAME NOT IN ('NOT STATED','Unknown','NOT STATED: Patient refused') 
          AND ETHNICITY_NAME IS NOT NULL 
        GROUP BY PATIENT_ID
    ) B ON A.PATIENT_ID = B.PATIENT_ID AND A.PRIMARY_ID = B.PRIMARY_ID

    UNION ALL

    SELECT 
        A."SK_PatientID" AS PATIENT_ID, 
        B."BK_EthnicityCode" AS ETHNICITY_CODE, 
        B."EthnicityDesc" AS ETHNICITY_DESC, 
        5 AS PRIORITY_RANK
    FROM DATA_LAKE.FACT_PATIENT."FactProfile" A
    INNER JOIN "Dictionary"."dbo"."Ethnicity" B ON A."SK_EthnicityID" = B."SK_EthnicityID"
    WHERE A."SK_DataSourceID" = 5
      AND A."PeriodEnd" = '9999-12-31 00:00:00.000'
      AND B."EthnicityDesc" NOT IN ('NOT STATED','Unknown','NOT STATED: Patient refused')
),

DEDUPED_ETHNICITY AS (
    SELECT PATIENT_ID, ETHNICITY_CODE, ETHNICITY_DESC
    FROM ALL_ETHNICITY_SOURCES
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PATIENT_ID ORDER BY PRIORITY_RANK ASC) = 1
),

--------------------------------------------------------------------------------
-- 2. DYNAMIC GP PRACTICE & PCN LOOKUP (Reference + ODS Fallback)
--------------------------------------------------------------------------------
DIM_PRACTICE_LOOKUP AS (
    SELECT 
        P.PRACTICE_CODE,
        P.PRACTICE_NAME,
        P.PCN_CODE,
        P.PCN_NAME,
        P.HEALTH_BOROUGH_NAME AS BOROUGH,
        P.REGISTERED_BOROUGH_NAME,
        W.WEIGHTED_PATIENTS_CORE AS WEIGHTED_LIST_SIZE,
        'REFERENCE' AS PRACTICE_METADATA_SOURCE
    FROM REFERENCE.PRIMARY_CARE.PCN_MEMBERSHIP_ALL P
    LEFT JOIN REFERENCE.PRIMARY_CARE.PRACTICE_WEIGHTED_POPULATION_CURRENT W
        ON P.PRACTICE_CODE = W.PRACTICE_CODE
    
    UNION ALL
    
    SELECT 
        O."Organisation_Code" AS PRACTICE_CODE,
        O."Organisation_Name" AS PRACTICE_NAME,
        NULL AS PCN_CODE,
        NULL AS PCN_NAME,
        NULL AS BOROUGH,
        NULL AS REGISTERED_BOROUGH_NAME,
        W.WEIGHTED_PATIENTS_CORE AS WEIGHTED_LIST_SIZE,
        'UKHFD' AS PRACTICE_METADATA_SOURCE
    FROM UKHFD.ODS."dim_GP_Practices_And_Prescribing_CCs_SCD" O
    LEFT JOIN REFERENCE.PRIMARY_CARE.PRACTICE_WEIGHTED_POPULATION_CURRENT W
        ON O."Organisation_Code" = W.PRACTICE_CODE
    WHERE O."Is_Latest" = 1
      AND O."Organisation_Code" NOT IN (
          SELECT PRACTICE_CODE 
          FROM REFERENCE.PRIMARY_CARE.PCN_MEMBERSHIP_ALL
      )
),

--------------------------------------------------------------------------------
-- 3. CSDS PATIENT CLEANUP & DIRECT ETHNICITY RESOLUTION
--------------------------------------------------------------------------------
RAW_PATIENT_CLEAN AS (
    SELECT 
        PAT."Unique_service_request_identifier",
        PAT."Pseudo_NHS_Number",
        PAT."Record_Number",
        CASE WHEN UPPER(PAT."GP_Practice_(Latest)_DV") = 'E84706' THEN 'E84015'
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
        CASE WHEN PAT."Age_of_patient_at_reporting_period_start_(Years)" < 0 OR PAT."Age_of_patient_at_reporting_period_start_(Years)" IS NULL THEN 'Unknown' ELSE AGEBAND."BK_AgeBand" END AS AGEBAND,
        AGEBAND."SK_AgeBandID",
        PAT."Ethnic_category" AS ETHNIC_CATEGORY_ORIG,
        ETH."EthnicityDesc" AS ETHNICITYDESC_ORIG,
        PAT."Ethnic_category" AS ETHNIC_CATEGORY_INIT,
        ETH."EthnicityDesc" AS ETHNICITYDESC_INIT,
        PAT."Lower_super_output_area_(Residence)",
        PAT."Person_stated_gender_code",
        PAT."Postcode_district",
        PAT."Local_authority_district_unitary_authority"
    FROM DATA_LAKE.CSDS_SIMPLE."tblPatient" PAT
    LEFT JOIN "Dictionary"."dbo"."Age" AGE ON PAT."Age_of_patient_at_reporting_period_start_(Years)" = AGE."SK_AgeID"
    LEFT JOIN "Dictionary"."dbo"."AgeBand" AGEBAND ON AGE."SK_AgeBandID" = AGEBAND."SK_AgeBandID"
    LEFT JOIN "Dictionary"."dbo"."Ethnicity" ETH ON PAT."Ethnic_category" = ETH."BK_EthnicityCode"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PAT."Unique_service_request_identifier" ORDER BY PAT."Record_Number" DESC) = 1
),

RESOLVED_PATIENT AS (
    SELECT 
        R.*,
        COALESCE(
            CASE 
                WHEN R.ETHNICITYDESC_INIT IN ('NOT STATED', 'Not Stated', 'Unknown') THEN E_FALLBACK.ETHNICITY_DESC
                ELSE R.ETHNICITYDESC_INIT 
            END,
            R.ETHNICITYDESC_INIT,
            'NOT STATED'
        ) AS RESOLVED_ETH_DESC,
        
        COALESCE(
            CASE 
                WHEN R.ETHNIC_CATEGORY_INIT IN ('Z', '99', 'Z0', 'Z*') THEN E_FALLBACK.ETHNICITY_CODE
                ELSE R.ETHNIC_CATEGORY_INIT 
            END,
            R.ETHNIC_CATEGORY_INIT,
            'Z'
        ) AS RESOLVED_ETH_CODE
    FROM RAW_PATIENT_CLEAN R
    LEFT JOIN DEDUPED_ETHNICITY E_FALLBACK ON R."Pseudo_NHS_Number" = E_FALLBACK.PATIENT_ID
),

--------------------------------------------------------------------------------
-- 4. HELPER CTEs (Deduplicated Care Contact, IMD, CYP101 & CYP102)
--------------------------------------------------------------------------------
TMP_CON AS (
    SELECT "Unique_service_request_identifier", "Time_between_referral_and_care_contact"
    FROM (
        SELECT 
            "Unique_service_request_identifier", 
            "Time_between_referral_and_care_contact",
            ROW_NUMBER() OVER (PARTITION BY "Unique_service_request_identifier" ORDER BY "Care_contact_date" ASC) AS RN
        FROM DATA_LAKE.CSDS_SIMPLE."tblCare_Contact"
        WHERE "Attended_or_did_not_attend_code" IN ('5','6')
    )
    WHERE RN = 1
),

TMP_IMD AS (
    SELECT *
    FROM DATA_LAKE__NCL.ANALYST_MANAGED.IMD_2025
    WHERE REGEXP_LIKE(
        LOCAL_AUTHORITY_DISTRICT_NAME_2024,
        'Barnet|Enfield|Camden|Islington|Haringey|Brent|Harrow|Hillingdon|Central London|West London|Hammersmith and Fulham|Hounslow|Ealing|Westminster|Kensington and Chelsea',
        'i'
    )
),

CYP102_DATA AS (
    SELECT 
        "UNIQUE SERVICE REQUEST IDENTIFIER", 
        "REFERRAL REJECTION REASON", 
        "REFERRAL CLOSURE REASON", 
        "REFERRAL REJECTION DATE", 
        "REFERRAL CLOSURE DATE"
    FROM DATA_LAKE.CSDS."CYP102ServiceTypeReferredTo"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY "UNIQUE SERVICE REQUEST IDENTIFIER" ORDER BY "CYP102 UNIQUE ID" DESC) = 1
),

DEDUPED_CYP101 AS (
    SELECT 
        "UNIQUE SERVICE REQUEST IDENTIFIER",
        "RECORD NUMBER",
        "PERSON ID",
        "AGE AT SERVICE REFERRAL RECEIVED DATE (YEARS)"
    FROM DATA_LAKE.CSDS."CYP101Referral"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY "UNIQUE SERVICE REQUEST IDENTIFIER", "RECORD NUMBER", "PERSON ID" ORDER BY "CYP101 UNIQUE ID" DESC) = 1
),

PV AS (
    SELECT DISTINCT 
        "RECORD NUMBER", 
        "PERSON ID", 
        "UNIQUE SERVICE REQUEST IDENTIFIER", 
        LEFT(CYP102."ORGANISATION CODE (PROVIDER)", 3) AS PROVIDERCODE, 
        'TRUE' AS "Post Covid flag"
    FROM DATA_LAKE.CSDS."CYP102ServiceTypeReferredTo" CYP102
    INNER JOIN NORMALISED_REFERRALS REF
        ON CYP102."RECORD NUMBER" = REF."Record_Number"
       AND CYP102."PERSON ID" = REF."Person_ID"
       AND CYP102."UNIQUE SERVICE REQUEST IDENTIFIER" = REF."Unique_service_request_identifier"
       AND LEFT(CYP102."ORGANISATION CODE (PROVIDER)", 3) = LEFT(REF."Organisation_identifier_(Code_of_provider)", 3)
    WHERE (LEFT(CYP102."ORGANISATION CODE (PROVIDER)", 3) = 'RYX' AND "CARE PROFESSIONAL TEAM LOCAL IDENTIFIER" IN ('2_157','2_310','2_459','2_553','2_711','2_764')) 
       OR (LEFT(CYP102."ORGANISATION CODE (PROVIDER)", 3) = 'RV3' AND "CARE PROFESSIONAL TEAM LOCAL IDENTIFIER" IN ('592363_556095651102','599565_556095651102','705299_556095651102','717245_556095651102','717246_556095651102','717266_556095651102','717267_556095651102')) 
       OR (LEFT(CYP102."ORGANISATION CODE (PROVIDER)", 3) = 'RKE' AND "CARE PROFESSIONAL TEAM LOCAL IDENTIFIER" IN ('PCSTE','PCSW','PCSWE','PCSNR','PCST','PCSASESS','PCSFU','PCSFUE')) 
       OR (LEFT(CYP102."ORGANISATION CODE (PROVIDER)", 3) IN ('RRP','RAP','RAL') AND "CARE PROFESSIONAL TEAM LOCAL IDENTIFIER" = 'MEPCST')
)

--------------------------------------------------------------------------------
-- 5. FINAL MAIN DATA ASSEMBLY
--------------------------------------------------------------------------------
SELECT 
    CURRENT_DATE() AS REFRESH_DATE,
    PAT1."Record_Number" AS RECORD_NUMBER,
    REF."Record_Number" AS PRIMARY_ID,
    REF."Unique_service_request_identifier" AS UNIQUE_ID,
    REF."Pseudo_NHS_Number" AS PATIENT_ID,
    REF."Person_ID" AS PERSON_ID,
    REF."Unique_LocalPatientId" AS LOCAL_PATIENTID,
    'CSDS-Simple-Referral' AS DATASET,
    'Referral' AS POD_GROUP,
    'Referral POD' AS POD,
    REPORTING.MAIN_DATA.DETERMINE_FISCAL_YEAR__CH_TEMP(REF."Referral_request_received_date") AS FIN_YEAR,
    MONTHNAME(REF."Referral_request_received_date") AS FIN_MONTH_TEXT,
    REPORTING.MAIN_DATA.DETERMINE_FISCAL_MONTH__CH_TEMP(REF."Referral_request_received_date") AS FIN_MONTH,
    REF."Referral_request_received_date" AS START_DATE,
    '' AS END_DATE,
    LAST_DAY(REF."Referral_request_received_date", 'WEEK') AS WEEKEND_DATE,
    CASE WHEN CYP102_DATA."REFERRAL REJECTION DATE" IS NOT NULL THEN 'Referrals Rejected'
         WHEN CYP102_DATA."REFERRAL REJECTION REASON" IS NOT NULL THEN 'Referrals Rejected'
         WHEN CYP102_DATA."REFERRAL CLOSURE DATE" IS NOT NULL THEN 'Referrals Discharged'
         WHEN CYP102_DATA."REFERRAL CLOSURE REASON" IS NOT NULL THEN 'Referrals Discharged'
         ELSE 'Open Referrals'
    END AS REFERRAL_STATUS,
    CYP102_DATA."REFERRAL REJECTION DATE" AS REFERRAL_REJECTION_DATE,
    CYP102_DATA."REFERRAL REJECTION REASON" AS REFERRAL_REJECTION_REASON,
    REF."Organisation_identifier_(Code_of_provider)" AS PROVIDER_CODE,
    COALESCE(
        ORG_PROVIDER.ORGANISATION_NAME,
        ORG_SITE.ORGANISATION_NAME,
        MISSING_CODES.NAME,
        'Unknown Provider'
    ) AS PROVIDER_NAME,
    'N/A' AS PROVIDER_SITE_CODE,
    'N/A' AS PROVIDER_SITE_NAME,
    REF."Organisation_code_(Code_of_commissioner)" AS COMMISSIONER_CODE,

COALESCE(
    COM.NAME,
    'Unknown Commissioner'
) AS COMMISSIONER_NAME,

    CASE WHEN PRAC.BOROUGH IS NULL THEN 'Non-WNL/Unknown/Invalid' ELSE PRAC.BOROUGH END AS BOROUGH,
    CASE WHEN PRAC.PCN_CODE IS NULL THEN 'Non-WNL/Unknown/Invalid' ELSE PRAC.PCN_NAME END AS PCN,
    CASE WHEN PRAC.PCN_CODE IS NULL THEN 'Non-WNL/Unknown/Invalid' ELSE PRAC.PCN_CODE END AS PCN_CODE,
    PAT1.GPCODE AS GP_PRACTICE_CODE,
    CASE WHEN PRAC.PRACTICE_NAME IS NULL THEN 'Non-WNL/Unknown/Invalid' ELSE PRAC.PRACTICE_NAME END AS GP_PRACTICE_NAME,
    PRAC.WEIGHTED_LIST_SIZE AS LIST_SIZE,
    PAT1."Age_of_patient_at_reporting_period_start_(Years)" AS AGE,
    CYP101."AGE AT SERVICE REFERRAL RECEIVED DATE (YEARS)" AS AGE_AT_REFERRAL_DATE_YEARS,
    PAT1."SK_AgeBandID" AS SK_AGE_BAND_CODE,
    PAT1.AGEBAND AS AGE_BAND,
    CASE WHEN PAT1."Age_of_patient_at_reporting_period_start_(Years)" BETWEEN 0 AND 17 THEN '0-17' 
         WHEN PAT1."Age_of_patient_at_reporting_period_start_(Years)" BETWEEN 18 AND 64 THEN '18-64'
         WHEN PAT1."Age_of_patient_at_reporting_period_start_(Years)" >= 65 THEN '65+'
    END AS AGE_GROUP,
    PAT1.RESOLVED_ETH_CODE AS ETHNICITY_CODE,
    PAT1.RESOLVED_ETH_DESC AS ETHNICITY_NAME,
    PAT1.ETHNIC_CATEGORY_ORIG AS ETHNIC_CATEGORY_ORIG,
    PAT1.ETHNICITYDESC_ORIG AS ETHNICITYDESC_ORIG,
    CASE WHEN PAT1.RESOLVED_ETH_DESC IS NULL THEN 'Unknown' ELSE INITCAP(SPLIT_PART(PAT1.RESOLVED_ETH_DESC, ':', 1)) END AS ETHNICITY_GROUPING,
    CASE WHEN PAT1.RESOLVED_ETH_DESC IS NULL THEN 'Not Stated/Unknown/Invalid'
         WHEN PAT1.RESOLVED_ETH_DESC IN ('Not stated','Not Known') THEN 'Not Stated/Unknown/Invalid'
         ELSE ETH."EthnicityDesc" 
    END AS ETHNICITY_SUB_GROUPING,
    PAT1."Person_stated_gender_code" AS GENDER_CODE,
    GEN."Gender" AS GENDER_NAME,
    PAT1."Lower_super_output_area_(Residence)" AS LSOA,
    IMD.INDEX_OF_MULTIPLE_DEPRIVATION_IMD_DECILE_WHERE_1_IS_MOST_DEPRIVED_10_PERCENT__OF_LSOAS AS DEPRIVATION_DECILE,
    CASE WHEN IMD.INDEX_OF_MULTIPLE_DEPRIVATION_IMD_DECILE_WHERE_1_IS_MOST_DEPRIVED_10_PERCENT__OF_LSOAS IN (1,2) THEN 'Most Deprived'
         WHEN IMD.INDEX_OF_MULTIPLE_DEPRIVATION_IMD_DECILE_WHERE_1_IS_MOST_DEPRIVED_10_PERCENT__OF_LSOAS IN (3,4) THEN 'Second Most Deprived'
         WHEN IMD.INDEX_OF_MULTIPLE_DEPRIVATION_IMD_DECILE_WHERE_1_IS_MOST_DEPRIVED_10_PERCENT__OF_LSOAS IN (5,6) THEN 'Third Most Deprived'
         WHEN IMD.INDEX_OF_MULTIPLE_DEPRIVATION_IMD_DECILE_WHERE_1_IS_MOST_DEPRIVED_10_PERCENT__OF_LSOAS IN (7,8) THEN 'Second Least Deprived'
         WHEN IMD.INDEX_OF_MULTIPLE_DEPRIVATION_IMD_DECILE_WHERE_1_IS_MOST_DEPRIVED_10_PERCENT__OF_LSOAS IN (9,10) THEN 'Least Deprived'
         WHEN IMD.INDEX_OF_MULTIPLE_DEPRIVATION_IMD_DECILE_WHERE_1_IS_MOST_DEPRIVED_10_PERCENT__OF_LSOAS IS NULL OR 
              IMD.INDEX_OF_MULTIPLE_DEPRIVATION_IMD_DECILE_WHERE_1_IS_MOST_DEPRIVED_10_PERCENT__OF_LSOAS = 0 THEN 'Unknown'
    END AS PATIENT_IMD_QUINTILE_25,
    CASE WHEN IMD.INDEX_OF_MULTIPLE_DEPRIVATION_IMD_DECILE_WHERE_1_IS_MOST_DEPRIVED_10_PERCENT__OF_LSOAS IN (1,2) THEN 5
         WHEN IMD.INDEX_OF_MULTIPLE_DEPRIVATION_IMD_DECILE_WHERE_1_IS_MOST_DEPRIVED_10_PERCENT__OF_LSOAS IN (3,4) THEN 4
         WHEN IMD.INDEX_OF_MULTIPLE_DEPRIVATION_IMD_DECILE_WHERE_1_IS_MOST_DEPRIVED_10_PERCENT__OF_LSOAS IN (5,6) THEN 3
         WHEN IMD.INDEX_OF_MULTIPLE_DEPRIVATION_IMD_DECILE_WHERE_1_IS_MOST_DEPRIVED_10_PERCENT__OF_LSOAS IN (7,8) THEN 2
         WHEN IMD.INDEX_OF_MULTIPLE_DEPRIVATION_IMD_DECILE_WHERE_1_IS_MOST_DEPRIVED_10_PERCENT__OF_LSOAS IN (9,10) THEN 1
         WHEN IMD.INDEX_OF_MULTIPLE_DEPRIVATION_IMD_DECILE_WHERE_1_IS_MOST_DEPRIVED_10_PERCENT__OF_LSOAS IS NULL OR 
              IMD.INDEX_OF_MULTIPLE_DEPRIVATION_IMD_DECILE_WHERE_1_IS_MOST_DEPRIVED_10_PERCENT__OF_LSOAS = 0 THEN 6
    END AS PATIENT_IMD_QUINTILE_25_ORDER,
    IMD.TOTAL_POPULATION_MID_2022 AS POP_BY_DEPRIVATION,
    PAT1."Postcode_district" AS PATIENT_POSTOCDE_DISTRICT,
    CON."Time_between_referral_and_care_contact" AS RESPONSE_TIME_DAYS,
    REF."Source_of_referral_for_community" AS SOURCE_OF_REFERRAL_CODE,
    CSLK1.DESCRIPTION AS SOURCE_OF_REFERRAL_NAME,
    REF."Referring_organisation_code" AS REFERRING_ORGANISATION_CODE,
    COALESCE(REF_ORG_EXACT.NAME,REF_ORG_PREFIX.NAME) AS REFERRING_ORGANISATION_NAME,
    REF."Primary_reason_for_referral_(Community_care)" AS PRIMARY_REASON_FOR_REFERRAL_CODE,
    CSLK2.DESCRIPTION AS PRIMARY_REASON_FOR_REFERRAL_DESC,
    REF.CLEAN_TEAM_CODE AS SERVICE_OR_TEAM_TYPE_REFERRED_TO_CODE,
    COALESCE(CSLK3.DESCRIPTION, 'Unknown Service') AS SERVICE_OR_TEAM_TYPE_REFERRED_TO_NAME,
    REF."Priority_type_code" AS PRIORITY_TYPE_CODE,
    PRIO."Display" AS PRIORITY_TYPE_DESC,
    CASE WHEN PAT1."Local_authority_district_unitary_authority" = 'E09000003' THEN 'Barnet'
         WHEN PAT1."Local_authority_district_unitary_authority" = 'E09000007' THEN 'Camden'
         WHEN PAT1."Local_authority_district_unitary_authority" = 'E09000010' THEN 'Enfield'
         WHEN PAT1."Local_authority_district_unitary_authority" = 'E09000014' THEN 'Haringey'
         WHEN PAT1."Local_authority_district_unitary_authority" = 'E09000019' THEN 'Islington' 
         WHEN PAT1."Local_authority_district_unitary_authority" = 'E09000005' THEN 'Brent' 
         WHEN PAT1."Local_authority_district_unitary_authority" = 'E09000009' THEN 'Ealing' 
         WHEN PAT1."Local_authority_district_unitary_authority" = 'E09000020' THEN 'West London'
         WHEN PAT1."Local_authority_district_unitary_authority" = 'E09000015' THEN 'Harrow' 
         WHEN PAT1."Local_authority_district_unitary_authority" = 'E09000018' THEN 'Hounslow' 
         WHEN PAT1."Local_authority_district_unitary_authority" = 'E09000017' THEN 'Hillingdon' 
         WHEN PAT1."Local_authority_district_unitary_authority" = 'E09000033' THEN 'Central London'
         WHEN PAT1."Local_authority_district_unitary_authority" = 'E09000013' THEN 'Hammersmith and Fulham' 
         ELSE 'Non-WNL residence'
    END AS BOROUGH_OF_RESIDENCE,
    REF."Waiting_time_measurement_type_(Community_Care)" AS WAITING_TIME_MEASUREMENT_TYPE_CODE,
    CSLK4.DESCRIPTION AS WAITING_TIME_MEASUREMENT_TYPE_DESC,
    CASE WHEN ("Referral_request_received_date" < '2023-04-01' AND "ServOrTeamTypeRefToCC_(Latest_List)_DV" IN ('45','51','52','53') AND "Waiting_time_measurement_type_(Community_Care)" IN ('05','5','07','7')
            OR "Referral_request_received_date" >= '2023-04-01' AND "Waiting_time_measurement_type_(Community_Care)" IN ('05','5','07','7')) 
          AND DATEDIFF(MINUTE, TIMESTAMP_FROM_PARTS("Referral_to_treatment_period_start_date", "Referaal_to_treatment_period_start_time"), TIMESTAMP_FROM_PARTS("Referral_to_treatment_period_end_date", REF."Referral_to_treatment_period_end_time")) BETWEEN 0 AND 120 
         THEN 1 
         ELSE 0
    END AS FLAG_FOR_2HR,
    CASE WHEN "Referral_request_received_date" < '2023-04-01' AND "ServOrTeamTypeRefToCC_(Latest_List)_DV" IN ('45','51','52','53') AND "Waiting_time_measurement_type_(Community_Care)" IN ('05','5','07','7') THEN 1
         WHEN "Referral_request_received_date" >= '2023-04-01' AND "Waiting_time_measurement_type_(Community_Care)" IN ('05','5','07','7') THEN 1 
         ELSE 0
    END AS UCR_REFERRALS,
    CASE WHEN "Referaal_to_treatment_period_start_time" IS NOT NULL THEN DATEDIFF(MINUTE, TIMESTAMP_FROM_PARTS("Referral_to_treatment_period_start_date", "Referaal_to_treatment_period_start_time"), TIMESTAMP_FROM_PARTS("Referral_to_treatment_period_end_date", "Referral_to_treatment_period_end_time")) END AS WAIT_MINS,
    CASE WHEN DATEDIFF(MINUTE, TIMESTAMP_FROM_PARTS("Referral_to_treatment_period_start_date", "Referaal_to_treatment_period_start_time"), TIMESTAMP_FROM_PARTS("Referral_to_treatment_period_end_date", "Referral_to_treatment_period_end_time")) BETWEEN 0 AND 15 THEN '0 to 15'
         WHEN DATEDIFF(MINUTE, TIMESTAMP_FROM_PARTS("Referral_to_treatment_period_start_date", "Referaal_to_treatment_period_start_time"), TIMESTAMP_FROM_PARTS("Referral_to_treatment_period_end_date", "Referral_to_treatment_period_end_time")) BETWEEN 15 AND 30 THEN '15 to 30'
         WHEN DATEDIFF(MINUTE, TIMESTAMP_FROM_PARTS("Referral_to_treatment_period_start_date", "Referaal_to_treatment_period_start_time"), TIMESTAMP_FROM_PARTS("Referral_to_treatment_period_end_date", "Referral_to_treatment_period_end_time")) BETWEEN 30 AND 45 THEN '30 to 45'
         WHEN DATEDIFF(MINUTE, TIMESTAMP_FROM_PARTS("Referral_to_treatment_period_start_date", "Referaal_to_treatment_period_start_time"), TIMESTAMP_FROM_PARTS("Referral_to_treatment_period_end_date", "Referral_to_treatment_period_end_time")) BETWEEN 45 AND 60 THEN '45 to 60'
         WHEN DATEDIFF(MINUTE, TIMESTAMP_FROM_PARTS("Referral_to_treatment_period_start_date", "Referaal_to_treatment_period_start_time"), TIMESTAMP_FROM_PARTS("Referral_to_treatment_period_end_date", "Referral_to_treatment_period_end_time")) BETWEEN 60 AND 75 THEN '60 to 75'
         WHEN DATEDIFF(MINUTE, TIMESTAMP_FROM_PARTS("Referral_to_treatment_period_start_date", "Referaal_to_treatment_period_start_time"), TIMESTAMP_FROM_PARTS("Referral_to_treatment_period_end_date", "Referral_to_treatment_period_end_time")) BETWEEN 75 AND 90 THEN '75 to 90'
         WHEN DATEDIFF(MINUTE, TIMESTAMP_FROM_PARTS("Referral_to_treatment_period_start_date", "Referaal_to_treatment_period_start_time"), TIMESTAMP_FROM_PARTS("Referral_to_treatment_period_end_date", "Referral_to_treatment_period_end_time")) BETWEEN 90 AND 105 THEN '90 to 105'
         WHEN DATEDIFF(MINUTE, TIMESTAMP_FROM_PARTS("Referral_to_treatment_period_start_date", "Referaal_to_treatment_period_start_time"), TIMESTAMP_FROM_PARTS("Referral_to_treatment_period_end_date", "Referral_to_treatment_period_end_time")) BETWEEN 105 AND 120 THEN '105 to 120'
         WHEN DATEDIFF(MINUTE, TIMESTAMP_FROM_PARTS("Referral_to_treatment_period_start_date", "Referaal_to_treatment_period_start_time"), TIMESTAMP_FROM_PARTS("Referral_to_treatment_period_end_date", "Referral_to_treatment_period_end_time")) > 120 THEN 'Over 120'
         ELSE 'Unknown' 
    END AS RTT_BY_WAITING_TIME_BAND,
    CASE WHEN CYP102_DATA."REFERRAL REJECTION DATE" IS NOT NULL OR CYP102_DATA."REFERRAL REJECTION REASON" IS NOT NULL THEN 'No - referrals rejected' ELSE 'Yes' END AS INCLUSION_FLAG,
    CASE WHEN ((REF."Referral_request_received_date" < '2023-04-01' AND REF."ServOrTeamTypeRefToCC_(Latest_List)_DV" IN ('45','51','52','53') AND "Waiting_time_measurement_type_(Community_Care)" IN ('05','5','07','7')) OR
               (REF."Referral_request_received_date" >= '2023-04-01' AND "Waiting_time_measurement_type_(Community_Care)" IN ('05','5','07','7'))) 
          AND REF."Unique_service_request_identifier" IN (SELECT "Unique_service_request_identifier" FROM DATA_LAKE.CSDS_SIMPLE."tblCare_Contact" WHERE "Activity_location_type_code" LIKE 'A%' OR "Activity_location_type_code" LIKE 'G%') 
          AND REF."Service_discharge_date" IS NOT NULL
         THEN 1 
         ELSE 0
    END AS HOMECARE_DISCHARGES,
    
    CASE WHEN PV."Post Covid flag" IS NULL THEN 'FALSE' ELSE PV."Post Covid flag" END AS IS_POST_COVID,
    CASE WHEN NGH_RES.NEIGHBOURHOOD_NAME IS NULL THEN 'Unknown' ELSE NGH_RES.NEIGHBOURHOOD_NAME END AS NEIGHBOURHOOD_OF_RESIDENCE,   
    CASE WHEN NGH_REG.neighbourhood_name IS NULL THEN 'Unknown' ELSE NGH_REG.neighbourhood_name END AS PRACTICE_NEIGHBOURHOOD,
    REF."Service_discharge_date" AS SERVICE_DISCHARGE_DATE,
    REPORTING.MAIN_DATA.DETERMINE_FISCAL_YEAR__CH_TEMP(REF."Referral_to_treatment_period_end_date") AS RTT_FIN_YEAR,
    MONTHNAME(REF."Referral_to_treatment_period_end_date") AS RTT_FIN_MONTH_TEXT,
    REPORTING.MAIN_DATA.DETERMINE_FISCAL_MONTH__CH_TEMP(REF."Referral_to_treatment_period_end_date") AS RTT_FIN_MONTH,
    1 AS ACTIVITY


FROM NORMALISED_REFERRALS REF


LEFT JOIN REFERENCE.ORGANISATION.ORGANISATION_NHS_PROVIDER ORG_PROVIDER
    ON REF."Organisation_identifier_(Code_of_provider)" = ORG_PROVIDER.ORGANISATION_CODE

LEFT JOIN REFERENCE.ORGANISATION.ORGANISATION_NHS_SITE ORG_SITE
    ON REF."Organisation_identifier_(Code_of_provider)" = ORG_SITE.ORGANISATION_CODE

LEFT JOIN MISSING_CODES_LOOKUP MISSING_CODES
    ON REF."Organisation_identifier_(Code_of_provider)" = MISSING_CODES.CODE

LEFT JOIN COMMISSIONER_LOOKUP COM
    ON UPPER(TRIM(
         REF."Organisation_code_(Code_of_commissioner)"
       )) = COM.CODE

LEFT JOIN REFERRING_ORGANISATION_LOOKUP REF_ORG_EXACT
    ON UPPER(TRIM(REF."Referring_organisation_code")) = REF_ORG_EXACT.CODE
LEFT JOIN REFERRING_ORGANISATION_LOOKUP REF_ORG_PREFIX
    ON UPPER(LEFT(TRIM(REF."Referring_organisation_code"), 3)) = REF_ORG_PREFIX.CODE
    
LEFT JOIN RESOLVED_PATIENT PAT1 ON REF."Unique_service_request_identifier" = PAT1."Unique_service_request_identifier"
LEFT JOIN PATIENT_DEDUPED PAT ON REF."Unique_service_request_identifier" = PAT."Unique_service_request_identifier"
LEFT JOIN "Dictionary".NELCSU."Pre_2020/21_LSOA_To_Commissioner" LSOA ON PAT1."Lower_super_output_area_(Residence)" = LSOA."LSOACode"
LEFT JOIN {{ ref('raw_dictionary_dbo_commissioner') }} COM2 ON LSOA."CommissionerCode" = COM2.Commissioner_Code
LEFT JOIN DIM_PRACTICE_LOOKUP PRAC ON PAT1.GPCODE = PRAC.PRACTICE_CODE 
LEFT JOIN "Dictionary"."dbo"."Ethnicity" ETH ON PAT1.RESOLVED_ETH_CODE = ETH."BK_EthnicityCode"
LEFT JOIN "Dictionary"."dbo"."Gender" GEN ON PAT1."Person_stated_gender_code" = GEN."GenderCode"
LEFT JOIN TMP_IMD IMD ON IMD.LSOA_CODE_2021 = PAT1."Lower_super_output_area_(Residence)"
LEFT JOIN REFERENCE.DATA_DICTIONARY.CSDS_SOURCE_OF_REFERRAL CSLK1 ON REF."Source_of_referral_for_community" = CSLK1.CODE
LEFT JOIN DATA_LAKE__NCL.ANALYST_MANAGED.CSDS_LOOKUP CSLK2 ON LTRIM(REF."Primary_reason_for_referral_(Community_care)", '0#') = CSLK2.CODE AND CSLK2."SIMPLETABLE_FIELDNAME" = 'Primary_reason_for_referral_(Community_care)'
LEFT JOIN REFERENCE.DATA_DICTIONARY.CSDS_SERVICE_OR_TEAM_TYPE CSLK3 ON REF.CLEAN_TEAM_CODE = CSLK3.CODE
LEFT JOIN DATA_LAKE__NCL.ANALYST_MANAGED.CSDS_LOOKUP CSLK4 ON RIGHT(REF."Waiting_time_measurement_type_(Community_Care)", 1) = CSLK4.CODE AND CSLK4."SIMPLETABLE_FIELDNAME" = 'Waiting_time_measurement_type_(Community_Care)'
LEFT JOIN "Dictionary"."E-Referral"."Priority" PRIO ON REF."Priority_type_code" = PRIO."Meaning"
LEFT JOIN TMP_CON CON ON REF."Unique_service_request_identifier" = CON."Unique_service_request_identifier"
LEFT JOIN CYP102_DATA ON REF."Unique_service_request_identifier" = CYP102_DATA."UNIQUE SERVICE REQUEST IDENTIFIER"
LEFT JOIN PV ON REF."Record_Number" = PV."RECORD NUMBER" 
            AND REF."Person_ID" = PV."PERSON ID" 
            AND REF."Unique_service_request_identifier" = PV."UNIQUE SERVICE REQUEST IDENTIFIER" 
            AND LEFT(REF."Organisation_identifier_(Code_of_provider)", 3) = PV.PROVIDERCODE
LEFT JOIN MODELLING.LOOKUP_WNL.VW_GP_PRACTICE NGH_REG ON PAT1.GPCODE = NGH_REG.PRACTICE_CODE
LEFT JOIN DATA_LAKE__NCL.ANALYST_MANAGED.NCL_NEIGHBOURHOOD_LSOA_2021 NGH_RES ON PAT1."Lower_super_output_area_(Residence)" = NGH_RES.LSOA_2021_CODE 
LEFT JOIN DEDUPED_CYP101 CYP101 ON REF."Unique_service_request_identifier" = CYP101."UNIQUE SERVICE REQUEST IDENTIFIER"
                                AND REF."Record_Number" = CYP101."RECORD NUMBER"
                                AND REF."Person_ID" = CYP101."PERSON ID"

WHERE REF."Referral_request_received_date" >= '2021-04-01'
