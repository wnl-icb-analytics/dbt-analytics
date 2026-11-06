{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
    )
}}
/*
MH/SMI record of alcohol consumption QOF Indicator MH007. Date of the alcohol consumption code ALC_COD
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/
WITH base_observations AS (
SELECT
    obs.id,
    obs.date_recorded,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.result_value,
    -- UCUM display where OLIDS maps the unit, else the recorded unit text. OLIDS leaves
    -- alcohol units per week unmapped (UCUM has no alcohol unit).
    COALESCE(obs.result_unit_display, NULLIF(TRIM(units.display), '')) AS result_unit_display
    FROM ({{ get_observations("'ALC_COD'") }}) obs
    LEFT JOIN {{ ref('stg_olids_concept') }} units
        ON obs.result_units_source_concept_id = units.concept_id
WHERE obs.clinical_effective_date IS NOT NULL 
AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
),
observations_with_demographics AS (
    SELECT DISTINCT
        bo.id,
        bo.date_recorded,
        bo.person_id,
        pd.gender,
        date(bo.clinical_effective_date) as clinical_effective_date,
        bo.concept_code,
        bo.concept_display,
        bo.result_value,
        CASE
WHEN LOWER(TRIM(bo.result_unit_display)) IN ('{units}/wk','units/w','upw','unit/wk','u/ week','u(nits)/we','units/week',
     'units pw','units per wk','units/wk','unt/wk','u/wkly','u/wee0') THEN 'U/week'
when bo.result_unit_display ILIKE '%/uweek' THEN 'U/week'
when bo.result_unit_display ILIKE ('%U/week') THEN 'U/week'
WHEN bo.result_unit_display ILIKE ('U/week%') THEN 'U/week' 
WHEN bo.result_unit_display ILIKE ('%unit%per%week%') THEN 'U/week' 
WHEN bo.result_unit_display ILIKE ('%unit%week%') THEN 'U/week' 
-- The concept fixes the unit as units per week.
WHEN bo.concept_code = '1082641000000106' AND LOWER(TRIM(bo.result_unit_display)) = 'per week' THEN 'U/week'
ELSE bo.result_unit_display END AS result_unit_display
    FROM base_observations bo
    LEFT JOIN {{ ref('dim_person_demographics') }} pd
        ON bo.person_id = pd.person_id
)
select
id AS observation_id,
date_recorded,
person_id,
gender,
clinical_effective_date,
concept_code,
concept_display,
result_value, 
result_unit_display,
CASE 
--EX DRINKER-----------------------------
--WHEN (concept_display ILIKE ANY ('Ex%', '%Abstinent%') OR concept_display = 'Stopped drinking alcohol') AND result_value = 0 AND result_unit_display = 'U/week' THEN 'Ex-Drinker'
WHEN concept_code in ('160582009','160585006','160587003','160584005','286857004','160583004','82581004','300939009','160579004') AND result_value = 0 AND result_unit_display  in ('U/week', 'per week') THEN 'Ex-Drinker'
--WHEN (concept_display ILIKE ANY ('Ex%', '%Abstinent%') OR concept_display = 'Stopped drinking alcohol') AND result_value IS NULL THEN 'Ex-Drinker'
WHEN concept_code in ('160582009','160585006','160587003','160584005','286857004','160583004','82581004','300939009','160579004')  AND result_value IS NULL THEN 'Ex-Drinker'
--NON DRINKER------------------------------------------------
--WHEN concept_display in ('Alcohol units consumed per week','Alcoholic beverage intake','Alcohol intake','Alcohol units consumed per day') AND result_value = 0 AND result_unit_display = 'U/week' THEN 'Non-Drinker'
WHEN concept_code in ('1082641000000106','897148007','160573003','1082631000000102') AND result_value = 0 AND result_unit_display in ('U/week', 'per week') THEN 'Non-Drinker'
--WHEN concept_display in ('Lifetime non-drinker of alcohol','Lifetime non-drinker','Current non-drinker of alcohol (finding)') AND result_value = 0 AND result_unit_display = 'U/week' THEN 'Non-Drinker'
WHEN concept_code in ('783261004','228274009','105542008') AND result_value = 0 AND result_unit_display in ('U/week', 'per week') THEN 'Non-Drinker'
--WHEN concept_display in ('Lifetime non-drinker of alcohol','Lifetime non-drinker','Current non-drinker of alcohol (finding)') AND result_value IS NULL THEN 'Non-Drinker'
WHEN concept_code in ('783261004','228274009','105542008') AND result_value IS NULL THEN 'Non-Drinker'
--HIGHER RISK-------------------------
--WHEN concept_display ILIKE ANY ('%Harmful%', '%Heavy%', '%Hazard%','%Higher%') AND result_value IS NULL THEN 'Higher Risk'
WHEN concept_code in ('198431000000105','86933000','160577002','228279004','228278007','160578007','198421000000108','777651000000101') AND result_value IS NULL THEN 'Higher Risk'
--INCREASING RISK --------------------
--WHEN concept_display ILIKE ANY ('%binge%', '%Increasing%', '%Moderate%','%Unhealthy%','%Problem%') AND result_value IS NULL THEN 'Increasing Risk'
WHEN concept_code in ('228317009','228326007','228316000','228315001','777631000000108','43783005','160576006','10939881000119105','160592001','228281002') AND result_value IS NULL THEN 'Increasing Risk'
--LOW RISK --------------------------------------
-- NHS/CMO: low risk up to 14 units a week; OHID higher risk above 35 (women) or 50 (men).
--WHEN concept_display ILIKE ANY ('%trivial%','%light%','%lower%') AND result_value IS NULL THEN 'Low Risk'
WHEN concept_code in ('266917007','228277002','160575005','777671000000105','160593006','228276006') AND result_value IS NULL THEN 'Low Risk'
WHEN concept_code in ('1082641000000106','897148007','160573003','219006','160589000','160591008','230086006','230085005','896792007','748381000000102','897148007','926322008','28127009','160588008','230088007','160590009') AND result_unit_display = 'U/week' AND result_value > 0 AND result_value <= 14 THEN 'Low Risk'
WHEN concept_code in ('1082641000000106','897148007','160573003','219006','160589000','160591008','230086006','230085005','896792007','748381000000102','897148007','926322008','28127009','160588008','230088007','160590009') AND result_unit_display = 'U/week' AND result_value > 14 AND result_value <= 35 AND GENDER = 'Female' THEN 'Increasing Risk'
WHEN concept_code in ('1082641000000106','897148007','160573003','219006','160589000','160591008','230086006','230085005','896792007','748381000000102','897148007','926322008','28127009','160588008','230088007','160590009') AND result_unit_display = 'U/week' AND result_value > 14 AND result_value <= 50 AND GENDER = 'Male' THEN 'Increasing Risk'
WHEN concept_code in ('1082641000000106','897148007','160573003','219006','160589000','160591008','230086006','230085005','896792007','748381000000102','897148007','926322008','28127009','160588008','230088007','160590009') AND result_unit_display = 'U/week' AND result_value > 35 AND GENDER = 'Female' THEN 'Higher Risk'
WHEN concept_code in ('1082641000000106','897148007','160573003','219006','160589000','160591008','230086006','230085005','896792007','748381000000102','897148007','926322008','28127009','160588008','230088007','160590009') AND result_unit_display = 'U/week' AND result_value > 50 AND GENDER = 'Male' THEN 'Higher Risk'
ELSE 'Unclear'
END AS alcohol_risk_category
from observations_with_demographics
QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, clinical_effective_date ORDER BY
    CASE alcohol_risk_category WHEN 'Higher Risk' THEN 1 WHEN 'Increasing Risk' THEN 2 WHEN 'Low Risk' THEN 3
         WHEN 'Ex-Drinker' THEN 4 WHEN 'Non-Drinker' THEN 5 ELSE 6 END,
    IFF(result_value IS NOT NULL, 0, 1),
    date_recorded DESC NULLS LAST, id DESC) = 1
