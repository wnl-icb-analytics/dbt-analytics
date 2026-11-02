{{
    config(
        materialized='view',
       tags=['fa_checks'])
}}
--replacement model in published layer for the wnl_fa_monthly view reporting.fa.wnl_fa_monthly.

--Gets the latest LSACM file for each provider.
WITH latest_files AS (
SELECT
    dv_provider_code,
    file_id AS fileid,
    CONCAT(financial_year,RIGHT(TO_VARCHAR(financial_year + 1), 2) ) AS financial_year,
    financial_month
FROM {{ ref('stg_sdl_meta_file_registry') }}
--FROM STAGING.SDL.STG_SDL_META_FILE_REGISTRY
WHERE dv_provider_code IN (
        'RAP','RAL','RKE','RRV','RAN','RP4','RP6','R1K','RYJ','RQM','RAS',
        'R1H', -- Barts
        'RJ1', -- GSTT
        'RJ7', -- St George's
        'RJZ', -- King's
        'RQX',  -- Homerton
        'RPY', -- The Royal Marsden NHS Foundation Trust
        'RTK', -- Ashford AND St Peter's Hospitals NHS Foundation Trust
        'RWG', -- West Hertfordshire Teaching Hospitals NHS Trust
        'RWH', -- East AND North Hertfordshire Teaching NHS Trust
          -- ,'NT411', -- Clementine Churchill Hospital - excluded as not in log processing event table
        'RDU') -- Frimley Health NHS Foundation Trust
AND feed = 'LSACM'
--latest financial year AND latest month submitted AND latest version submitted
AND financial_year = (SELECT MAX(financial_year) FROM {{ ref('stg_sdl_meta_file_registry') }} WHERE feed = 'LSACM')
--AND financial_year = (SELECT MAX(financial_year) FROM STAGING.SDL.STG_SDL_META_FILE_REGISTRY WHERE feed = 'LSACM')
QUALIFY ROW_NUMBER() OVER (PARTITION BY dv_provider_code ORDER BY financial_month DESC,file_version DESC) = 1
)
--make a new freeze file CTE from latest LSACM - historical files from 2024-25 onwards
,freeze_files AS (
select distinct fileid 
from (
select acm.dv_provider_code, acm.dv_financial_year, acm.dv_financial_month, MAX(acm.meta_file_id) AS fileid
FROM {{ ref('stg_lsacm_latest')}} acm
--from STAGING.SLAM.STG_LSACM_LATEST acm
WHERE dv_financial_year >= '202425' 
AND acm.dv_provider_code IN (
        'RAP','RAL','RKE','RRV','RAN','RP4','RP6','R1K','RYJ','RQM','RAS',
        'R1H', -- Barts
        'RJ1', -- GSTT
        'RJ7', -- St George's
        'RJZ', -- King's
        'RQX',  -- Homerton
        'RPY', -- The Royal Marsden NHS Foundation Trust
        'RTK', -- Ashford AND St Peter's Hospitals NHS Foundation Trust
        'RWG', -- West Hertfordshire Teaching Hospitals NHS Trust
        'RWH', -- East AND North Hertfordshire Teaching NHS Trust
          -- ,'NT411', -- Clementine Churchill Hospital - excluded as not in log processing event table
        'RDU')
    GROUP BY ALL
    ) a
)
-- filtered acm simplified to remove the UNION 
,filtered_acm AS (

 SELECT acm.*
    FROM {{ ref('stg_lsacm')}} acm
    --FROM STAGING.SLAM.STG_LSACM acm
    INNER JOIN freeze_files fr
        ON acm.meta_file_id = fr.fileid
    INNER JOIN latest_files lf
        ON acm.dv_provider_code = lf.dv_provider_code
    WHERE acm.commissioner_code IN (
        '07X','07M','08H','08D','07R','93C','07P','07W','07Y',
        '08C','08E','08G','08Y','09A','W2U3Z','Y05','T04',
        'Z9B2Z','QMJ','QRV','13R','13Q'
    )
    AND (
        /* Retain complete earlier financial years */
        acm.dv_financial_year < lf.financial_year
        /* For 202627, retain only months up to that provider's latest month */
        OR (
            acm.dv_financial_year = lf.financial_year
            AND acm.dv_financial_month <= lf.financial_month
        )
    )
)
--Main SELECT:
    
SELECT 
    acm.point_of_delivery_code,
    acm.local_point_of_delivery_code,
    CASE --outpatients
        WHEN tariff_code ILIKE any ('WF01B%','WF02B%','WF01D%','WF02D%') then 'Outpatient Firsts'
        WHEN tariff_code ILIKE any ('WF01A%','WF02A%','WF01C%','WF02C%') then 'Outpatient Follow-ups'
        WHEN pod_group = 'Outpatients' then iff(hrg.hrg_code is not null,'Outpatient Procedures','Outpatient Other')
        --maternity/NELNE
        WHEN left(tariff_code,4) in ('NZ01','NZ02','NZ03','NZ11','NZ12','NZ13','NZ14','NZ15','NZ25'
                                     ,'NZ30','NZ31','NZ32','NZ33','NZ34','NZ40','NZ41','NZ42','NZ43'
                                     ,'NZ44','NZ50','NZ51')
                                     then 'Maternity - Deliveries'
        WHEN tariff_code ILIKE 'NZ%' then 'Maternity - Non-deliveries'
        WHEN acm.point_of_delivery_code in ('NELNE','NELNEXBD') or acm.point_of_delivery_code = 'NEL' AND tariff_code ILIKE 'PB%' then 'Neonates AND Transfers'
        WHEN pod_group = 'Maternity' then 'Maternity - Pathways'
        --ambulatory AND SDEC
        WHEN dv_provider_code = 'RKE' AND local_sub_specialty_code in ('100AMB','300AMB') then 'Ambulatory Care'
        WHEN dv_provider_code = 'RRV' AND point_of_delivery_further_detail_code in ('SDEC','CYPSDEC') then 'Same Day Emergency Care'
        WHEN dv_provider_code = 'RYJ' AND tariff_code = 'AEC' then 'Same Day Emergency Care'
        WHEN dv_provider_code = 'RAL' AND tariff_code = 'SDECED' then 'Same Day Emergency Care'
        WHEN dv_provider_code = 'RAS' AND tariff_code = 'SDEC' then 'Same Day Emergency Care'
        ELSE pod_group
    END AS podgroupoverview,
    point_of_delivery_further_detail_code,
    acm.local_point_of_delivery_description,
    point_of_delivery_further_detail_description,
    tariff_code,
    dv_actual_market_forces_factor,
    activity_treatment_function_code,
    main_specialty_code,
    to_varchar(local_sub_specialty_code) AS local_sub_specialty_code,
    to_varchar(
        decode(
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
    ) AS finmonth_label,

    acm.dv_financial_month AS dv_financialmonth,
    acm.dv_financial_year AS dv_financialyear,
    acm.dv_financial_year AS financialyear,
    commissioner_code,
    dv_provider_code,
    acm.site_of_treatment_code,
    reporting_type_indicator AS dv_reporting_type,
    org1.organisation_name,
    org2.organisation_name AS organisation_site_name,
    CASE
        WHEN commissioner_code in (
            '07X','07M','08H','08D','07R','93C','07P','07W','07Y',
            '08C','08E','08G','08Y','09A','W2U3Z','Y05','T04',
            'Z9B2Z','QMJ','QRV'
        ) then 'WNL'
        WHEN commissioner_code in ('13R','13Q') then 'NHSE'
        ELSE 'OTHER'
    END AS wnl_flag,
    acm.commissioned_service_category_code,
    acm.service_code,
    acm.dv_actual_activity AS activity,
    acm.dv_actual_price AS price,
    dv_planned_activity AS activity_plan,
    dv_planned_price AS price_plan,
    tariff_code_description AS tariff_description,
    hrg.hrg_chapter,
    hrg.hrg_subchapter,
    sp.specialty_name,
    sp.specialty_category,
    CASE
        WHEN left(acm.dv_provider_code, 3) in ('RAL', 'RAP') then 'RFL'
        WHEN left(acm.dv_provider_code, 3) in ('RKE','RRV','RAN','RP4','RP6','R1K','RYJ','RQM','RAS')
        then prov.reporting_shorthAND ELSE org1.organisation_name
    END AS providername,
    CASE
        WHEN left(acm.dv_provider_code, 3) in ('RAP','RAL','RKE','RRV','RAN','RP4','RP6','R1K','RYJ','RQM','RAS') then 'WNL'
        ELSE 'Major OOS'
    END AS providertype,
    CASE
        WHEN (
            acm.point_of_delivery_code in ('UNBCHEMO','UNBRAD','UNBRTHPY','OPUNB','UNBOTHER', 'IPUNB','UNBPALL','OPUNBRAD','DRUG','DEVICE')
            or acm.local_point_of_delivery_code in ('AIP','PIP','DACARD')
            or acm.tariff_code like 'RDN%'
            or acm.tariff_code like 'RDP%'
        )
        AND (
            acm.tariff_code not in ('RA28Z','RA29Z')
            or acm.tariff_code is null
        )
        AND (
            acm.point_of_delivery_further_detail_code not in ('FIN-INT','FIN-TBFT','FIN-DIRACC','FIN-COMMID')
            or acm.point_of_delivery_further_detail_code is null
        )
        AND (
            acm.tariff_code not like 'SD%'
            or acm.tariff_code is null
        )
        then 'Variable'
        WHEN (
            acm.point_of_delivery_code in ('EL','ELXBD','DC','OPPROC','OPPROCFA','OPPROCFUP','OPFAMPCL','OPFASPCL')
            or left(acm.tariff_code, 5) in ('WF01B','WF02B','WF01D','WF02D')
        )
        then 'Elective' ELSE 'Fixed'
    END AS api_type,
    --new speccommflag logic to account for delegation and CSCC codes
    CASE
        WHEN acm.commissioner_code in ('13Q','13R') then 'Retained'
        WHEN acm.service_code = 'NULL' or acm.service_code is null or acm.service_code = '99999' then 'ICB-commissioned'
        WHEN upper(acm.service_code) like '%XXX' AND ser.service_code is not null then cs.funding_status
        WHEN ser.icb_delegation_26_27 <> 'BLUE' then ser.current_funding_status
        WHEN ser.icb_delegation_26_27 = 'BLUE' then cs.funding_status
        ELSE 'Invalid Service Code'
    END AS speccommflag,
    CASE WHEN tariff_code = 'UZ01Z' then 'Uncoded HRG'
      WHEN ifnull(national_tariff_indicator,national_tariff_pb_r_flag) in ('Y','y','1') then 'National'
      WHEN ifnull(national_tariff_indicator,national_tariff_pb_r_flag) is null      then 'National'
      WHEN ifnull(national_tariff_indicator,national_tariff_pb_r_flag) in ('N','n','0') then 'Local'
      ELSE 'Unknown' END costing_type

from filtered_acm acm
LEFT JOIN {{ ref('stg_dictionary_dbo_organisation')}} org1
--LEFT JOIN STAGING.DICTIONARY.STG_DICTIONARY_DBO_ORGANISATION ORG1
    on acm.dv_provider_code = org1.organisation_code

LEFT JOIN {{ ref('stg_dictionary_dbo_organisation')}} org2
--LEFT JOIN STAGING.DICTIONARY.STG_DICTIONARY_DBO_ORGANISATION ORG2
    on acm.site_of_treatment_code = org2.organisation_code

LEFT JOIN {{ ref('stg_reference_pod_group_mapping')}} pod
--LEFT JOIN STAGING.REFERENCE.STG_REFERENCE_POD_GROUP_MAPPING POD
    on CONCAT(
        coalesce(acm.point_of_delivery_code, '?'),
        coalesce(acm.local_point_of_delivery_code, '?'),
        coalesce(acm.local_point_of_delivery_description, '?')
    ) = pod.pod_lookup

LEFT JOIN {{ ref('stg_dictionary_dbo_hrg')}} hrg
--LEFT JOIN STAGING.DICTIONARY.STG_DICTIONARY_DBO_HRG HRG
    on left(acm.tariff_code,5) = hrg.hrg_code

LEFT JOIN {{ ref('stg_dictionary_dbo_specialties')}} sp
--LEFT JOIN STAGING.DICTIONARY.STG_DICTIONARY_DBO_SPECIALTIES SP
    on cASe
           WHEN acm.dv_provider_code = 'RAP'
               then left(
                   coalesce(
                       acm.main_specialty_code,
                       acm.activity_treatment_function_code,
                       acm.local_sub_specialty_code
                   ),
                   3
               )
           ELSE left(
                   coalesce(
                      acm.main_specialty_code,
                       acm.local_sub_specialty_code,
                       acm.activity_treatment_function_code
                   ),
                   3
               )
       END = sp.bk_specialty_code
LEFT JOIN {{ ref('stg_reference_wnl_reporting_names') }} prov
--LEFT JOIN STAGING.REFERENCE.STG_REFERENCE_WNL_REPORTING_NAMES prov
    on IFF(
        acm.dv_provider_code = 'RAL'
        AND acm.site_of_treatment_code = 'RAL27',
        'RAL27',
        acm.dv_provider_code
    ) = prov.reporting_code
--replaced with latest file
LEFT JOIN {{ ref('pss_service_code') }} ser 
--LEFT JOIN REFERENCE.DATA_DICTIONARY.PSS_SERVICE_CODE SER 
ON UPPER(acm.service_code) = ser.service_code
--added extra file for CSCC codes AND delegation
LEFT JOIN {{ ref('nhs_dd_commissioned_service_category') }} cs 
--LEFT JOIN REFERENCE.DATA_DICTIONARY.NHS_DD_COMMISSIONED_SERVICE_CATEGORY cs 
on acm.commissioned_service_category_code = cs.code

