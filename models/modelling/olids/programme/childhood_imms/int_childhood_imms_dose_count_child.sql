{{
    config(
        materialized='table',
        tags=['childhood_imms'])

}}
--This table creates dose count and date labels for childhood immunisations for children aged under 11 years old using a base table

SELECT DISTINCT
v.PERSON_ID 
,v.age
,v.imd_quintile
,v.imdquintile_order
,v.ethnicity_category
,v.ethcat_order
,v.BOROUGH_REGISTERED
,v.borough_resident
,v.residential_loc
,v.practice_code
,sixin1_dose1_label
,sixin1_dose1_fiscal
,sixin1_dose1_sort
,sixin1_dose2_label
,sixin1_dose2_fiscal
,sixin1_dose2_sort
,sixin1_dose3_label
,sixin1_dose3_fiscal
,sixin1_dose3_sort
,sixin1_dose4_label
,sixin1_dose4_fiscal
,sixin1_dose4_sort
,rota_dose1_label
,rota_dose1_fiscal
,rota_dose1_sort
,rota_dose2_label
,rota_dose2_fiscal
,rota_dose2_sort
,menb_dose1_label
,menb_dose1_fiscal
,menb_dose1_sort
,menb_dose2_label
,menb_dose2_fiscal
,menb_dose2_sort
,menb_dose3_label
,menb_dose3_fiscal
,menb_dose3_sort
,pcv_dose1_label
,pcv_dose1_fiscal
,pcv_dose1_sort
,pcv_dose2_label
,pcv_dose2_fiscal
,pcv_dose2_sort
,hibmc_dose1_label
,hibmc_dose1_fiscal
,hibmc_dose1_sort
,mmr_dose1_label
,mmr_dose1_fiscal
,mmr_dose1_sort
,mmr_dose2_label
,mmr_dose2_fiscal
,mmr_dose2_sort
,fourin1_dose1_label
,fourin1_dose1_fiscal
,fourin1_dose1_sort
,mmrv_dose1_label
,mmrv_dose1_fiscal
,mmrv_dose1_sort
,mmrv_dose2_label
,mmrv_dose2_fiscal
,mmrv_dose2_sort
FROM {{ ref('int_childhood_imms_dose_base_child') }} v
--from DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_BASE_CHILD v
LEFT JOIN {{ ref('int_childhood_imms_historical_sixin1_dose1') }} s1 using (PERSON_ID)
--left join DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_HISTORICAL_SIXIN1_DOSE1 s1 using (PERSON_ID)
LEFT JOIN {{ ref('int_childhood_imms_historical_sixin1_dose2') }} s2 using (PERSON_ID)
--left join DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_HISTORICAL_SIXIN1_DOSE2 s2 using (PERSON_ID)
LEFT JOIN {{ ref('int_childhood_imms_historical_sixin1_dose3') }} s3 using (PERSON_ID)
--left join DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_HISTORICAL_SIXIN1_DOSE3 s3 using (PERSON_ID)
LEFT JOIN {{ ref('int_childhood_imms_historical_sixin1_dose4') }} s4 using (PERSON_ID)
--left join DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_HISTORICAL_SIXIN1_DOSE3 s3 using (PERSON_ID)
LEFT JOIN {{ ref('int_childhood_imms_historical_rota_dose1') }} r1 using (PERSON_ID)
--left join DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_HISTORICAL_ROTA_DOSE1 r1 using (PERSON_ID)
LEFT JOIN {{ ref('int_childhood_imms_historical_rota_dose2') }} r2 using (PERSON_ID)
--left join DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_HISTORICAL_ROTA_DOSE2 r2 using (PERSON_ID)
LEFT JOIN {{ ref('int_childhood_imms_historical_menb_dose1') }} m1 using (PERSON_ID)
--left join DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_HISTORICAL_MENB_DOSE1 m1 using (PERSON_ID)
LEFT JOIN {{ ref('int_childhood_imms_historical_menb_dose2') }} m2 using (PERSON_ID)
--left join DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_HISTORICAL_MENB_DOSE2 m2 using (PERSON_ID)
LEFT JOIN {{ ref('int_childhood_imms_historical_menb_dose3') }} m3 using (PERSON_ID)
--left join DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_HISTORICAL_MENB_DOSE3 m3 using (PERSON_ID)
LEFT JOIN {{ ref('int_childhood_imms_historical_pcv_dose1') }} p1 using (PERSON_ID)
--left join DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_HISTORICAL_PCV_DOSE1 p1 using (PERSON_ID) 
LEFT JOIN {{ ref('int_childhood_imms_historical_pcv_dose2') }} p2 using (PERSON_ID)
--left join DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_HISTORICAL_PCV_DOSE2 p2 using (PERSON_ID) 
LEFT JOIN {{ ref('int_childhood_imms_historical_hibmenc_dose1') }} h using (PERSON_ID)
--left join DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_HISTORICAL_HIBMENC_DOSE1 h using (PERSON_ID)
LEFT JOIN {{ ref('int_childhood_imms_historical_mmr_dose1') }} mr1 using (PERSON_ID)
--left join DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_HISTORICAL_MMR_DOSE1 mr1 using (PERSON_ID)
LEFT JOIN {{ ref('int_childhood_imms_historical_mmr_dose2') }} mr2 using (PERSON_ID)
--left join DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_HISTORICAL_MMR_DOSE2 mr2 using (PERSON_ID)
left join {{ ref('int_childhood_imms_historical_fourin1_dose1') }} f1 using (PERSON_ID)
--left join DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_HISTORICAL_FOURIN1_DOSE1 f using (PERSON_ID)
left join {{ ref('int_childhood_imms_historical_mmrv_dose1') }} mv1 using (PERSON_ID)
--left join DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_HISTORICAL_MMRV_DOSE1 mv1 using (PERSON_ID)
left join {{ ref('int_childhood_imms_historical_mmrv_dose2') }} mv2 using (PERSON_ID)
--left join DEV__MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_HISTORICAL_MMRV_DOSE2 mv2 using (PERSON_ID)
