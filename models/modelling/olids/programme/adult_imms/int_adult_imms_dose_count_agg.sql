{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}

SELECT 
p.fiscal_year
,p.vaccination_dose
,CASE
WHEN p.vaccination_dose = 'PPV Dose 1' THEN 1
WHEN p.vaccination_dose = 'Shingles Dose 1' THEN 2
WHEN p.vaccination_dose = 'Shingles Dose 2' THEN 3
WHEN p.vaccination_dose = 'RSV Dose 1' THEN 4
END As VACC_ORDER 
,p.practice_code
,p.month_year
,p.month_label
,p.age_band_5y
,p.age_65_plus
,p.age_75_plus
,p.is_care_home_resident
,p.is_immunosuppressed
,p.in_ppv_clinical_risk_group
,p.in_rsv_clinical_risk_group
,p.is_pregnant
,p.turn_65_after_Sep_2023
,p.ethnicity_category
,p.ethcat_order
,p.imd_quintile
,p.imdquintile_order
,p.vaccination_count 
FROM (
------- ADULT IMMUNISATION DOSE COUNTS (65+ POPULATION)
--PPV Dose 1
select 
 'PPV Dose 1' as vaccination_dose, ppv_dose1_fiscal as fiscal_year,  practice_code, ppv_dose1_sort as month_year, ppv_dose1_label as month_label,  age_band_5y, age_65_plus, age_75_plus, is_care_home_resident, is_immunosuppressed, in_ppv_clinical_risk_group, in_rsv_clinical_risk_group, is_pregnant, turn_65_after_Sep_2023, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, count(person_id) as vaccination_count
FROM {{ ref('int_adult_imms_dose_count') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_DOSE_COUNT
where ppv_dose1_sort is not null
group by all

UNION

--Shingles Dose 1
select 
 'Shingles Dose 1' as vaccination_dose, shing_dose1_fiscal as fiscal_year,  practice_code, shing_dose1_sort as month_year, shing_dose1_label as month_label,  age_band_5y, age_65_plus, age_75_plus, is_care_home_resident, is_immunosuppressed, in_ppv_clinical_risk_group, in_rsv_clinical_risk_group, is_pregnant, turn_65_after_Sep_2023, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, count(person_id) as vaccination_count
FROM {{ ref('int_adult_imms_dose_count') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_DOSE_COUNT
where shing_dose1_sort is not null
group by all

UNION

--Shingles Dose 2
select 
 'Shingles Dose 2' as vaccination_dose, shing_dose2_fiscal as fiscal_year,  practice_code, shing_dose2_sort as month_year, shing_dose2_label as month_label,  age_band_5y, age_65_plus, age_75_plus, is_care_home_resident, is_immunosuppressed, in_ppv_clinical_risk_group, in_rsv_clinical_risk_group, is_pregnant, turn_65_after_Sep_2023, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, count(person_id) as vaccination_count
FROM {{ ref('int_adult_imms_dose_count') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_DOSE_COUNT
where shing_dose2_sort is not null
group by all

UNION

--RSV Dose 1
select 
 'RSV Dose 1' as vaccination_dose, rsv_dose1_fiscal as fiscal_year,  practice_code, rsv_dose1_sort as month_year, RSV_DOSE1_LABEL as month_label,  age_band_5y, age_65_plus, age_75_plus, is_care_home_resident, is_immunosuppressed, in_ppv_clinical_risk_group, in_rsv_clinical_risk_group, is_pregnant, turn_65_after_Sep_2023, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, count(person_id) as vaccination_count
FROM {{ ref('int_adult_imms_dose_count') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_ADULT_IMMS_DOSE_COUNT
where rsv_dose1_sort is not null
group by all

) p


