{{
    config(
        materialized='view',
       tags=['fa_checks'])
}}
--REPLACEMENT MODEL IN PUBLISHED LAYER FOR THE WNL_FA_MONTHLY VIEW REPORTING.FA.WNL_FA_MONTHLY.


--Gets the latest LSACM file for each provider.
WITH latest_files AS (
SELECT
    dv_provider_code,
    file_id AS fileid,
    financial_month
FROM {{ ref('stg_sdl_meta_file_registry') }}
--FROM STAGING.SDL.STG_SDL_META_FILE_REGISTRY
WHERE DV_PROVIDER_CODE IN (
        'RAP','RAL','RKE','RRV','RAN','RP4','RP6','R1K','RYJ','RQM','RAS',
        'R1H', -- Barts
        'RJ1', -- GSTT
        'RJ7', -- St George's
        'RJZ', -- King's
        'RQX',  -- Homerton
        'RPY', -- The Royal Marsden NHS Foundation Trust
        'RTK', -- Ashford and St Peter's Hospitals NHS Foundation Trust
        'RWG', -- West Hertfordshire Teaching Hospitals NHS Trust
        'RWH', -- East and North Hertfordshire Teaching NHS Trust
          -- ,'NT411', -- Clementine Churchill Hospital - excluded as not in log processing event table
        'RDU') -- Frimley Health NHS Foundation Trust
AND feed = 'LSACM'
--latest financial year and latest month submitted and latest version submitted
AND financial_year = (SELECT MAX(financial_year) FROM S{{ ref('stg_sdl_meta_file_registry') }} WHERE feed = 'LSACM')
--AND financial_year = (SELECT MAX(financial_year) FROM STAGING.SDL.STG_SDL_META_FILE_REGISTRY WHERE feed = 'LSACM')
QUALIFY ROW_NUMBER() OVER (PARTITION BY dv_provider_code ORDER BY financial_month DESC,file_version DESC) = 1
)
/*
    filtered_acm:
    This is the main performance improvement.
    Instead of joining all of LSACM to all lookup tables first, we first reduce LSACM down to only the rows we actually want.
    The original WHERE was logically:
        frozen-file rows
        OR
        latest-file rows
    That kind of OR can make Snowflake do more work.
    Here the two branches are split out clearly.
    Initially UNION was used, not UNION ALL, because the original OR would not duplicate a row
    if it satisfied both branches. UNION preserved that behaviour.
    However, UNION has been changed to UNION ALL for slightly improved performance, as the two branches shouldn't overlap as the latest is using fileIDs
*/
,filtered_acm AS (
    /*
        Branch 1:
        Rows from the M12 freeze file list.
        Original logic:
            FREEZE.FILEID IS NOT NULL
            AND ACM.FINANCIAL_YEAR >= 202425
            AND provider in your provider list
    */
    --CAN'T DO THIS SECTION WITHOUT THE FREEZE FILE IN DBT
--     SELECT 
--         ACM.*
--     FROM  {{ ref('stg_lsacm') }} acm
--     --FROM STAGING.SLAM.STG_LSACM ACM
--     --The original query used LEFT JOIN FREEZE then WHERE FREEZE.FILEID IS NOT NULL. That is effectively an INNER JOIN, so we make it explicit here.
--     INNER JOIN DATA_LAKE__NCL.ANALYST_MANAGED.LSACM_M12_FREEZE_FILE_IDS FREEZE
--         ON ACM.META_FILE_ID = FREEZE.FILEID
--      WHERE ACM.DV_FINANCIAL_YEAR >= 202425
--         AND ACM.DV_PROVIDER_CODE in (
--           'RAP','RAL','RKE','RRV','RAN','RP4','RP6','R1K','RYJ','RQM','RAS',
--           'R1H', -- Barts
--           'RJ1', -- GSTT
--           'RJ7', -- St George's
--           'RJZ', -- King's
--           'RQX',  -- Homerton
--           'RPY', -- The Royal Marsden NHS Foundation Trust
--           'RTK', -- Ashford and St Peter's Hospitals NHS Foundation Trust
--           'RWG', -- West Hertfordshire Teaching Hospitals NHS Trust
--           'RWH', -- East and North Hertfordshire Teaching NHS Trust
--             -- ,'NT411', -- Clementine Churchill Hospital - excluded as not in log processing event                 table
--           'RDU' -- Frimley Health NHS Foundation Trust
--       )
--           --Commissioner filter applied inside this branch, so it reduces rows before joins.
--      AND ACM.COMMISSIONER_CODE in (
--           '07X','07M','08H','08D','07R','93C','07P','07W','07Y',
--           '08C','08E','08G','08Y','09A','W2U3Z','Y05','T04',
--           'Z9B2Z','QMJ','QRV',
--           '13R','13Q'
--       )
-- --19,700,580 million rows
--     UNION ALL
    /*
        Branch 2:
        Rows from the latest available file per provider.
        Original logic:
            META_FILE_ID IN latest files AND provider in your provider list
        Added logic:
            AND NOT (ACM.FINANCIAL_YEAR = 202627 AND ACM.FINANCIAL_MONTH > 2)
        This now applies only to this latest-file branch, matching where it had been placed before.
    */
    SELECT 
        ACM.*
    FROM {{ ref('stg_lsacm') }} acm
    --FROM STAGING.SLAM.STG_LSACM ACM
    INNER JOIN latest_files LF ON ACM.META_FILE_ID = LF.FILEID
    AND ACM.DV_PROVIDER_CODE in (
          'RAP','RAL','RKE','RRV','RAN','RP4','RP6','R1K','RYJ','RQM','RAS',
          'R1H', -- Barts
          'RJ1', -- GSTT
          'RJ7', -- St George's
          'RJZ', -- King's
          'RQX',  -- Homerton
          'RPY', -- The Royal Marsden NHS Foundation Trust
          'RTK', -- Ashford and St Peter's Hospitals NHS Foundation Trust
          'RWG', -- West Hertfordshire Teaching Hospitals NHS Trust
          'RWH', -- East and North Hertfordshire Teaching NHS Trust
            -- ,'NT411', -- Clementine Churchill Hospital - excluded as not in log processing event                 table
          'RDU' -- Frimley Health NHS Foundation Trust
      )
    --This is one of the lines that was slowing things down in the old query. It is now applied before all the lookup joins, and only within the latest-file branch.
      AND NOT (
          ACM.DV_FINANCIAL_YEAR = 202627
          AND ACM.DV_FINANCIAL_MONTH > 6
      )
      /*
          Commissioner filter applied inside this branch too.
          This avoids the SQL precedence issue where:
              A OR B AND commissioner_filter
          is interpreted as:
              A OR (B AND commissioner_filter)
          Here it clearly applies to both branches.
      */
      AND ACM.COMMISSIONER_CODE in (
          '07X','07M','08H','08D','07R','93C','07P','07W','07Y',
          '08C','08E','08G','08Y','09A','W2U3Z','Y05','T04',
          'Z9B2Z','QMJ','QRV',
          '13R','13Q'
      )
      --5,259,172 rows
)
/*
    Main SELECT:
    This is the original SELECT, but now it reads from filtered_acm instead of the raw LSACM table.
    The important change is:
        FROM filtered_acm ACM
    So all the joins below only happen after LSACM has already been reduced.
*/

SELECT 
    ACM.POINT_OF_DELIVERY_CODE,
    ACM.LOCAL_POINT_OF_DELIVERY_CODE,
    CASE --Outpatients
     WHEN TARIFF_CODE ILIKE ANY ('WF01B%','WF02B%','WF01D%','WF02D%') THEN 'Outpatient Firsts'
     WHEN TARIFF_CODE ILIKE ANY ('WF01A%','WF02A%','WF01C%','WF02C%') THEN 'Outpatient Follow-ups'
     WHEN POD_GROUP = 'Outpatients' THEN IFF(HRG.HRG_CODE IS NOT NULL,'Outpatient Procedures','Outpatient Other')
     --Maternity/NELNE
     WHEN LEFT(TARIFF_CODE,4) IN ('NZ01','NZ02','NZ03','NZ11','NZ12','NZ13','NZ14','NZ15','NZ25'
                                 ,'NZ30','NZ31','NZ32','NZ33','NZ34','NZ40','NZ41','NZ42','NZ43'
                                 ,'NZ44','NZ50','NZ51')
                                 THEN 'Maternity - Deliveries'
     WHEN TARIFF_CODE ILIKE 'NZ%' THEN 'Maternity - Non-deliveries'
     WHEN ACM.POINT_OF_DELIVERY_CODE IN ('NELNE','NELNEXBD') OR ACM.POINT_OF_DELIVERY_CODE = 'NEL' AND TARIFF_CODE ILIKE 'PB%' THEN 'Neonates and Transfers'
     WHEN POD_GROUP = 'Maternity' THEN 'Maternity - Pathways'
     --Ambulatory and SDEC
     WHEN DV_PROVIDER_CODE = 'RKE' AND LOCAL_SUB_SPECIALTY_CODE IN ('100AMB','300AMB') THEN 'Ambulatory Care'
     WHEN DV_PROVIDER_CODE = 'RRV' AND POINT_OF_DELIVERY_FURTHER_DETAIL_CODE IN ('SDEC','CYPSDEC') THEN 'Same Day Emergency Care'
     WHEN DV_PROVIDER_CODE = 'RYJ' AND TARIFF_CODE = 'AEC' THEN 'Same Day Emergency Care'
     WHEN DV_PROVIDER_CODE = 'RAL' AND TARIFF_CODE = 'SDECED' THEN 'Same Day Emergency Care'
     WHEN DV_PROVIDER_CODE = 'RAS' AND TARIFF_CODE = 'SDEC' THEN 'Same Day Emergency Care'
     
     ELSE POD_GROUP
     END AS PODGROUPOVERVIEW,
    POINT_OF_DELIVERY_FURTHER_DETAIL_CODE,
    ACM.LOCAL_POINT_OF_DELIVERY_DESCRIPTION,
    POINT_OF_DELIVERY_FURTHER_DETAIL_DESCRIPTION,
    TARIFF_CODE,
    DV_ACTUAL_MARKET_FORCES_FACTOR,
    ACTIVITY_TREATMENT_FUNCTION_CODE,
    MAIN_SPECIALTY_CODE,
    TO_VARCHAR(LOCAL_SUB_SPECIALTY_CODE) AS LOCAL_SUB_SPECIALTY_CODE,
    TO_VARCHAR(
        DECODE(
            acm.dv_financial_month,
            1,  '01-Apr',
            2,  '02-May',
            3,  '03-Jun',
            4,  '04-Jul',
            5,  '05-Aug',
            6,  '06-Sep',
            7,  '07-Oct',
            8,  '08-Nov',
            9,  '09-Dec',
            10, '10-Jan',
            11, '11-Feb',
            12, '12-Mar'
        )
    ) AS "FinMonth",

    ACM.DV_FINANCIAL_MONTH AS DV_FINANCIALMONTH,
    ACM.DV_FINANCIAL_YEAR AS DV_FINANCIALYEAR,
    ACM.DV_FINANCIAL_YEAR AS FINANCIALYEAR,
    COMMISSIONER_CODE,
    DV_PROVIDER_CODE,
    ACM.SITE_OF_TREATMENT_CODE, 
    REPORTING_TYPE_INDICATOR AS DV_REPORTING_TYPE,
    ORG1.ORGANISATION_NAME, 
    ORG2.ORGANISATION_NAME AS ORGANISATION_SITE_NAME,
    CASE 
        WHEN COMMISSIONER_CODE IN (
            '07X','07M','08H','08D','07R','93C','07P','07W','07Y',
            '08C','08E','08G','08Y','09A','W2U3Z','Y05','T04',
            'Z9B2Z','QMJ','QRV'
        ) THEN 'WNL'
        WHEN COMMISSIONER_CODE IN ('13R','13Q') THEN 'NHSE'
        ELSE 'OTHER' 
    END AS "WNL_FLAG",

    ACM.COMMISSIONED_SERVICE_CATEGORY_CODE,
    ACM.SERVICE_CODE,
    ACM.DV_ACTUAL_ACTIVITY AS ACTIVITY,
    ACM.DV_ACTUAL_PRICE AS PRICE,
    DV_PLANNED_ACTIVITY AS ACTIVITY_PLAN, 
    DV_PLANNED_PRICE AS PRICE_PLAN,
    TARIFF_CODE_DESCRIPTION AS TARIFF_DESCRIPTION, 
    HRG.HRG_CHAPTER, 
    HRG.HRG_SUBCHAPTER, 
    SP.SPECIALTY_NAME, 
    SP.SPECIALTY_CATEGORY,
    CASE 
        WHEN LEFT(ACM.DV_PROVIDER_CODE, 3) IN ('RAL', 'RAP') 
            THEN 'RFL'

        WHEN LEFT(ACM.DV_PROVIDER_CODE, 3) IN (
            'RKE','RRV','RAN','RP4','RP6','R1K','RYJ','RQM','RAS'
        )
            THEN Prov.REPORTING_SHORTHAND

        ELSE ORG1.ORGANISATION_NAME
    END AS PROVIDERNAME,

    CASE
        WHEN LEFT(ACM.DV_PROVIDER_CODE, 3) IN (
            'RAP','RAL','RKE','RRV','RAN','RP4','RP6','R1K','RYJ','RQM','RAS'
        ) THEN 'WNL'

        ELSE 'Major OOS'
    END AS PROVIDERTYPE,

    CASE
        WHEN (
            ACM.POINT_OF_DELIVERY_CODE IN (
                'UNBCHEMO','UNBRAD','UNBRTHPY','OPUNB','UNBOTHER',
                'IPUNB','UNBPALL','OPUNBRAD','DRUG','DEVICE'
            )
            OR ACM.LOCAL_POINT_OF_DELIVERY_CODE IN ('AIP','PIP','DACARD')
            OR ACM.TARIFF_CODE LIKE 'RDN%' 
            OR ACM.TARIFF_CODE LIKE 'RDP%'
        )
        AND (
            ACM.TARIFF_CODE NOT IN ('RA28Z','RA29Z') 
            OR ACM.TARIFF_CODE IS NULL
        )
        AND (
            ACM.POINT_OF_DELIVERY_FURTHER_DETAIL_CODE NOT IN (
                'FIN-INT','FIN-TBFT','FIN-DIRACC','FIN-COMMID'
            ) 
            OR ACM.POINT_OF_DELIVERY_FURTHER_DETAIL_CODE IS NULL
        )
        AND (
            ACM.TARIFF_CODE NOT LIKE 'SD%' 
            OR ACM.TARIFF_CODE IS NULL
        )
        THEN 'Variable'

        WHEN (
            ACM.POINT_OF_DELIVERY_CODE IN (
                'EL','ELXBD','DC','OPPROC','OPPROCFA','OPPROCFUP','OPFAMPCL','OPFASPCL'
            )
            OR LEFT(ACM.TARIFF_CODE, 5) IN ('WF01B','WF02B','WF01D','WF02D')
        )

        /*
            These are your previously commented API Type exclusions.

            They are inside the SELECT CASE, so they classify rows.
            They do not reduce the number of rows returned.
        */

        /*
        AND (
            LEFT(ACM.TARIFF_CODE, 5) NOT IN (
                'MA50Z','MA51Z','MA52A','MA52B','MA53Z',
                'MA54Z','MA55A','MA55B','MA56A','MA56B'
            ) 
            OR ACM.TARIFF_CODE IS NULL
        )

        AND (
            ACM.ACTIVITY_TREATMENT_FUNCTION_CODE NOT IN (
                '501','560','700','710','711','712','713','715',
                '720','721','722','723','724','725','726','727',
                '199','499'
            )
            OR ACM.ACTIVITY_TREATMENT_FUNCTION_CODE IS NULL
        )
*/
        THEN 'Elective' ELSE 'Fixed'  END AS "API Type",
     CASE
        WHEN ACM.COMMISSIONER_CODE IN ('13Q','13R') 
            THEN 'Retained'
        WHEN ACM.SERVICE_CODE = 'NCBPSXXX' THEN
             CASE WHEN COMMISSIONED_SERVICE_CATEGORY_CODE = '21' THEN 'Retained'
                  WHEN COMMISSIONED_SERVICE_CATEGORY_CODE = '25' THEN 'Delegated'
                  ELSE 'Retained' END
        WHEN ACM.SERVICE_CODE LIKE '%TOP%' THEN
             CASE WHEN COMMISSIONED_SERVICE_CATEGORY_CODE = '21' THEN 'Retained'
                  WHEN COMMISSIONED_SERVICE_CATEGORY_CODE = '25' THEN 'Delegated'
                  WHEN COMMISSIONED_SERVICE_CATEGORY_CODE = '12' THEN 'ICB-commissioned'
                  ELSE 'Retained' END
        WHEN ACM.SERVICE_CODE IN ('CCGPS08S_EXC','99999999','NULL') 
             OR ACM.SERVICE_CODE LIKE '%99999%' 
             OR ACM.SERVICE_CODE IS NULL 
            THEN 'ICB-commissioned'
        WHEN ACM.DV_FINANCIAL_YEAR <= '202526' THEN 
            CASE 
                WHEN SER.ICB_DELEGATION_25_26 IN ('RED','BLUE','NOT APPLICABLE') 
                    THEN 'Retained'

                WHEN SER.ICB_DELEGATION_25_26 = 'GREEN' 
                    THEN 'Delegated'

                ELSE 'Invalid Service Code'
            END
        WHEN ACM.DV_FINANCIAL_YEAR = '202627' THEN 
            CASE 
                WHEN SER.ICB_DELEGATION_26_27 IN ('RED','BLUE','NOT APPLICABLE') 
                    THEN 'Retained'

                WHEN SER.ICB_DELEGATION_26_27 = 'GREEN' 
                    THEN 'Delegated'
                ELSE 'Invalid Service Code'
            END
        ELSE 'Invalid Service Code' 
    END AS SPECCOMMFLAG
--new version of spec_comm - if happy then replace above logic with this
    ,CASE
    WHEN acm.service_code IN ('CCGPS08S_EXC','99999999','NULL') OR acm.service_code IS NULL OR acm.service_code LIKE '%99999%' THEN 'ICB-commissioned'
    WHEN acm.COMMISSIONER_CODE IN ('13Q','13R') THEN 'Retained' 
    WHEN UPPER(acm.service_code) LIKE '%XXX' THEN cs.funding_status 
    WHEN ser.service_code IS NOT NULL AND UPPER(acm.service_code) NOT LIKE '%XXX' AND ser.icb_delegation_26_27 <> 'BLUE' THEN ser.current_funding_status
    WHEN ser.service_code IS NOT NULL AND UPPER(acm.service_code) NOT LIKE '%XXX' AND ser.icb_delegation_26_27 = 'BLUE' THEN cs.funding_status
    ELSE 'Invalid Service Code' 
    END AS spec_comm_flagv2

,CASE WHEN TARIFF_CODE = 'UZ01Z' THEN 'Uncoded HRG'
      WHEN IFNULL(NATIONAL_TARIFF_INDICATOR,NATIONAL_TARIFF_PB_R_FLAG) IN ('Y','y','1') THEN 'National'
      WHEN IFNULL(NATIONAL_TARIFF_INDICATOR,NATIONAL_TARIFF_PB_R_FLAG) IS NULL      THEN 'National'
      WHEN IFNULL(NATIONAL_TARIFF_INDICATOR,NATIONAL_TARIFF_PB_R_FLAG) IN ('N','n','0') THEN 'Local'
      ELSE 'Unknown' END COSTING_TYPE

FROM filtered_acm ACM
--Lookup joins now happen after filtering ACM.
LEFT JOIN {{ ref('stg_dictionary_dbo_organisation')}} org1
--LEFT JOIN STAGING.DICTIONARY.STG_DICTIONARY_DBO_ORGANISATION ORG1
    ON ACM.DV_PROVIDER_CODE = ORG1.ORGANISATION_CODE

LEFT JOIN {{ ref('stg_dictionary_dbo_organisation')}} org2
--LEFT JOIN STAGING.DICTIONARY.STG_DICTIONARY_DBO_ORGANISATION ORG2
    ON ACM.SITE_OF_TREATMENT_CODE = ORG2.ORGANISATION_CODE

--POD lookup join. Kept as per the original logic.
LEFT JOIN {{ ref('stg_reference_pod_group_mapping')}} pod
--LEFT JOIN DEV__STAGING.REFERENCE.STG_REFERENCE_POD_GROUP_MAPPING POD
    ON CONCAT(
        COALESCE(ACM.POINT_OF_DELIVERY_CODE, '?'),
        COALESCE(ACM.LOCAL_POINT_OF_DELIVERY_CODE, '?'),
        COALESCE(ACM.LOCAL_POINT_OF_DELIVERY_DESCRIPTION, '?')
    ) = POD.POD_LOOKUP

LEFT JOIN {{ ref('stg_dictionary_dbo_hrg')}} hrg
--LEFT JOIN STAGING.DICTIONARY.STG_DICTIONARY_DBO_HRG HRG
    ON LEFT(ACM.TARIFF_CODE,5) = HRG.HRG_CODE

LEFT JOIN {{ ref('stg_dictionary_dbo_specialties')}} sp
--LEFT JOIN STAGING.DICTIONARY.STG_DICTIONARY_DBO_SPECIALTIES SP
    ON CASE 
           WHEN ACM.DV_PROVIDER_CODE = 'RAP'
               THEN LEFT(
                   COALESCE(
                       ACM.MAIN_SPECIALTY_CODE, 
                       ACM.ACTIVITY_TREATMENT_FUNCTION_CODE, 
                       ACM.LOCAL_SUB_SPECIALTY_CODE
                   ), 
                   3
               )
           ELSE LEFT(
                   COALESCE(
                      ACM.MAIN_SPECIALTY_CODE, 
                       ACM.LOCAL_SUB_SPECIALTY_CODE, 
                       ACM.ACTIVITY_TREATMENT_FUNCTION_CODE
                   ), 
                   3
               )
       END = SP.BK_SPECIALTY_CODE
LEFT JOIN {{ ref('stg_reference_wnl_reporting_names') }} prov
--LEFT JOIN STAGING.REFERENCE.STG_REFERENCE_WNL_REPORTING_NAMES Prov
    ON IFF(
        ACM.DV_PROVIDER_CODE = 'RAL' 
        AND ACM.SITE_OF_TREATMENT_CODE = 'RAL27',
        'RAL27',
        ACM.DV_PROVIDER_CODE
    ) = Prov.REPORTING_CODE
--replaced with latest file
LEFT JOIN {{ ref('pss_service_code') }} SER 
--LEFT JOIN REFERENCE.DATA_DICTIONARY.PSS_SERVICE_CODE SER 
ON UPPER(ACM.SERVICE_CODE) = SER.SERVICE_CODE
--added extra file for CSCC codes and delegation
LEFT JOIN {{ ref('nhs_dd_commissioned_service_category') }} cs 
--LEFT JOIN REFERENCE.DATA_DICTIONARY.NHS_DD_COMMISSIONED_SERVICE_CATEGORY cs 
on acm.commissioned_service_category_code = cs.code

