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
{% set alcohol_unit = "((arbitrary|alcohol|uk|ukalcohol)?units?|u|unt|\\\\{units\\\\}|\\\\[arb''u\\\\]|units?of(alcohol|wine|beer|spirits))" %}
WITH base_observations AS (
SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.result_value AS original_result_value,
    -- Prefer source text: a mapped display can lose the original time basis or quantity.
    COALESCE(NULLIF(TRIM(units.display), ''), obs.result_unit_display) AS source_result_unit_display
    FROM ({{ get_observations("'ALC_COD'") }}) obs
    LEFT JOIN {{ ref('stg_olids_concept') }} units
        ON obs.result_units_source_concept_id = units.concept_id
WHERE obs.clinical_effective_date IS NOT NULL
AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
),
unit_labels AS (
    SELECT *,
        LOWER(REPLACE(TRIM(source_result_unit_display), ' ', '')) AS unit_label,
        concept_code IN ('1082641000000106','897148007','160573003','219006','160589000','160591008','230086006','230085005','896792007','748381000000102','926322008','28127009','160588008','230088007','160590009', '1082631000000102') AS is_numeric_units_concept,
        COALESCE(unit_label, '') IN ('', 'u', 'unit', 'units', '{units}', 'arbitraryunit', 'arbitraryunits',
            '[arb''u]', 'alcoholunit', 'alcoholunits', 'unitsofalcohol', 'ukunits', 'ukalcoholunits') AS has_bare_alcohol_unit,
        COALESCE(unit_label IN ('{units}/d', '{units}/day', '[arb''u]/d', 'a/day', 'u/day]')
            OR REGEXP_LIKE(unit_label,
                '([~]?[0-9.]*{{ alcohol_unit }}?(/per|per/|/|per)(d|days?|daily)|{{ alcohol_unit }}?daily)([.]|ontypicalday|typicalday)?'),
            FALSE) AS has_daily_unit,
        COALESCE(REGEXP_LIKE(unit_label,
                '([~]?[0-9.]*{{ alcohol_unit }}(/per|per/|/|per)?|(/per|per/|/|per)?)(w|wk|wks|week|weeks|weekly|wkly)[.]?')
            -- A bare number can describe a multi-week period or a different quantity.
            OR (original_result_value = 0 AND unit_label IN ('0/week', '0u/week'))
            OR unit_label IN ('unitspw', 'upw', 'u(nits)/we', 'u/wee0'), FALSE) AS has_weekly_unit_text,
        has_weekly_unit_text
            -- Non-zero coded status still needs an alcohol quantity, not just a time basis.
            AND (is_numeric_units_concept OR NOT REGEXP_LIKE(unit_label,
                '(/per|per/|/|per)?(w|wk|wks|week|weeks|weekly|wkly)[.]?')) AS has_weekly_unit
    FROM base_observations
),
observations_with_demographics AS (
    SELECT DISTINCT
        bo.id,
        bo.person_id,
        pd.gender,
        date(bo.clinical_effective_date) as clinical_effective_date,
        bo.concept_code,
        bo.concept_display,
        bo.original_result_value,
        bo.source_result_unit_display,
        COALESCE(bo.original_result_value IS DISTINCT FROM 0 AND (
            (bo.concept_code = '1082631000000102' AND bo.has_weekly_unit)
            OR (bo.concept_code = '1082641000000106' AND bo.has_daily_unit)), FALSE) AS is_daily_unit_conflict,
        bo.original_result_value IS NOT NULL AND NOT is_daily_unit_conflict
            AND ((bo.concept_code = '1082631000000102' AND bo.has_bare_alcohol_unit)
                OR (bo.is_numeric_units_concept AND bo.has_daily_unit
                    AND bo.concept_code <> '1082641000000106')) AS is_converted_from_daily,
        IFF(is_converted_from_daily, bo.original_result_value * 7, bo.original_result_value) AS result_value,
        CASE WHEN is_daily_unit_conflict AND bo.concept_code = '1082631000000102' THEN 'Daily concept with weekly unit; not converted'
            WHEN is_daily_unit_conflict THEN 'Weekly concept with daily unit; not converted'
            WHEN is_converted_from_daily AND bo.concept_code = '1082631000000102' THEN 'Daily concept multiplied by 7'
            WHEN is_converted_from_daily THEN 'Explicit daily unit multiplied by 7'
            ELSE 'No daily conversion' END AS conversion_reason,
        bo.is_numeric_units_concept,
        bo.has_weekly_unit_text,
        CASE
WHEN is_converted_from_daily THEN 'U/week'
WHEN bo.has_weekly_unit THEN 'U/week'
WHEN bo.concept_code = '1082641000000106' AND bo.has_bare_alcohol_unit THEN 'U/week'
-- Zero is unchanged by the time basis, even when the concept and unit disagree.
WHEN bo.original_result_value = 0 AND bo.is_numeric_units_concept AND bo.has_daily_unit THEN 'U/week'
ELSE bo.source_result_unit_display END AS result_unit_display
    FROM unit_labels bo
    LEFT JOIN {{ ref('dim_person_demographics') }} pd
        ON bo.person_id = pd.person_id
)
select
person_id,
gender,
clinical_effective_date,
concept_code,
concept_display,
result_value,
result_unit_display,
original_result_value,
source_result_unit_display,
is_converted_from_daily,
is_daily_unit_conflict,
conversion_reason,
CASE
WHEN is_daily_unit_conflict THEN 'Unclear'

--EX DRINKER-----------------------------
--WHEN (concept_display ILIKE ANY ('Ex%', '%Abstinent%') OR concept_display = 'Stopped drinking alcohol') AND result_value = 0 AND result_unit_display = 'U/week' THEN 'Ex-Drinker'
WHEN concept_code in ('160582009','160585006','160587003','160584005','286857004','160583004','82581004','300939009','160579004') AND result_value = 0
    AND (result_unit_display = 'U/week' OR has_weekly_unit_text) THEN 'Ex-Drinker'
--WHEN (concept_display ILIKE ANY ('Ex%', '%Abstinent%') OR concept_display = 'Stopped drinking alcohol') AND result_value IS NULL THEN 'Ex-Drinker'
WHEN concept_code in ('160582009','160585006','160587003','160584005','286857004','160583004','82581004','300939009','160579004')  AND result_value IS NULL THEN 'Ex-Drinker'
--NON DRINKER------------------------------------------------
--WHEN concept_display in ('Alcohol units consumed per week','Alcoholic beverage intake','Alcohol intake','Alcohol units consumed per day') AND result_value = 0 AND result_unit_display = 'U/week' THEN 'Non-Drinker'
WHEN concept_code in ('1082641000000106','897148007','160573003','1082631000000102') AND result_value = 0 AND result_unit_display = 'U/week' THEN 'Non-Drinker'
--WHEN concept_display in ('Lifetime non-drinker of alcohol','Lifetime non-drinker','Current non-drinker of alcohol (finding)') AND result_value = 0 AND result_unit_display = 'U/week' THEN 'Non-Drinker'
WHEN concept_code in ('783261004','228274009','105542008') AND result_value = 0
    AND (result_unit_display = 'U/week' OR has_weekly_unit_text) THEN 'Non-Drinker'
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
WHEN is_numeric_units_concept AND result_unit_display = 'U/week' AND result_value > 0 AND result_value <= 14 THEN 'Low Risk'
WHEN is_numeric_units_concept AND result_unit_display = 'U/week' AND result_value > 14 AND result_value <= 35 AND GENDER = 'Female' THEN 'Increasing Risk'
WHEN is_numeric_units_concept AND result_unit_display = 'U/week' AND result_value > 14 AND result_value <= 50 AND GENDER = 'Male' THEN 'Increasing Risk'
WHEN is_numeric_units_concept AND result_unit_display = 'U/week' AND result_value > 35 AND GENDER = 'Female' THEN 'Higher Risk'
WHEN is_numeric_units_concept AND result_unit_display = 'U/week' AND result_value > 50 AND GENDER = 'Male' THEN 'Higher Risk'
ELSE 'Unclear'
END AS alcohol_risk_category
from observations_with_demographics
QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, clinical_effective_date ORDER BY
    CASE alcohol_risk_category WHEN 'Higher Risk' THEN 1 WHEN 'Increasing Risk' THEN 2 WHEN 'Low Risk' THEN 3
         WHEN 'Ex-Drinker' THEN 4 WHEN 'Non-Drinker' THEN 5 ELSE 6 END,
    IFF(result_value IS NOT NULL, 0, 1),
    id) = 1
