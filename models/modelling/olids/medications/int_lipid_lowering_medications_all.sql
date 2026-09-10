{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All lipid-regulating medication orders (BNF section 2.12): statins, ezetimibe,
bempedoic acid, PCSK9 inhibitors, inclisiran, fibrates, bile acid sequestrants,
nicotinic acid derivatives and omega-3 products.

Legacy BNF codes place every chemical in the section under prefix 0212000, so the
section filter is the class definition. lipid_lowering_class splits the section by
chemical substance code so consumers can choose the products that count as
lipid-lowering therapy for their purpose.

Excludes orders dated before 1990 or after the build date; the source carries both.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH base_orders AS (
    SELECT
        person_id,
        medication_order_id,
        medication_statement_id,
        order_date,
        order_medication_name,
        order_dose,
        order_quantity_value,
        order_quantity_unit,
        order_duration_days,
        statement_medication_name,
        mapped_concept_code,
        mapped_concept_display,
        bnf_code,
        bnf_name,
        -- Chemical substance: legacy BNF positions 1-9
        LEFT(bnf_code, 9) AS bnf_chemical_code
    FROM (
        {{ get_medication_orders(bnf_code='0212') }}
    )
    WHERE order_date BETWEEN '1990-01-01' AND CURRENT_DATE()
),

classified AS (
    SELECT
        *,
        CASE
            WHEN bnf_chemical_code IN (
                '0212000B0',  -- Atorvastatin
                '0212000AA',  -- Rosuvastatin
                '0212000Y0',  -- Simvastatin
                '0212000X0',  -- Pravastatin
                '0212000M0',  -- Fluvastatin
                '0212000C0'   -- Cerivastatin (withdrawn)
            ) THEN 'STATIN'
            WHEN bnf_chemical_code IN (
                '0212000AC',  -- Simvastatin / ezetimibe
                '0212000AJ'   -- Simvastatin / fenofibrate
            ) THEN 'STATIN_COMBINATION'
            WHEN bnf_chemical_code = '0212000L0' THEN 'EZETIMIBE'
            WHEN bnf_chemical_code IN (
                '0212000AK',  -- Bempedoic acid
                '0212000AL'   -- Bempedoic acid / ezetimibe
            ) THEN 'BEMPEDOIC_ACID'
            WHEN bnf_chemical_code IN (
                '0212000AH',  -- Evolocumab
                '0212000AI'   -- Alirocumab
            ) THEN 'PCSK9_INHIBITOR'
            WHEN bnf_chemical_code = '0212000AM' THEN 'INCLISIRAN'
            WHEN bnf_chemical_code IN (
                '0212000D0',  -- Bezafibrate
                '0212000P0',  -- Fenofibrate
                '0212000Q0',  -- Gemfibrozil
                '021200010'   -- Ciprofibrate
            ) THEN 'FIBRATE'
            WHEN bnf_chemical_code IN (
                '0212000F0',  -- Colestyramine
                '0212000K0',  -- Colestipol
                '0212000AD'   -- Colesevelam
            ) THEN 'BILE_ACID_SEQUESTRANT'
            WHEN bnf_chemical_code IN (
                '0212000A0',  -- Acipimox
                '0212000U0',  -- Nicotinic acid
                '0212000AE'   -- Nicotinic acid / laropiprant
            ) THEN 'NICOTINIC_ACID'
            WHEN bnf_chemical_code IN (
                '0212000AB',  -- Omega-3-acid ethyl esters
                '0212000V0',  -- Omega-3 marine triglycerides
                '0212000AN'   -- Icosapent ethyl
            ) THEN 'OMEGA_3'
            ELSE 'OTHER'     -- e.g. ispaghula husk
        END AS lipid_lowering_class
    FROM base_orders
)

SELECT
    person_id,
    medication_order_id,
    medication_statement_id,
    order_date,
    order_medication_name,
    order_dose,
    order_quantity_value,
    order_quantity_unit,
    order_duration_days,
    statement_medication_name,
    mapped_concept_code,
    mapped_concept_display,
    bnf_code,
    bnf_name,
    bnf_chemical_code,
    lipid_lowering_class,
    lipid_lowering_class IN ('STATIN', 'STATIN_COMBINATION') AS is_statin,

    -- Statin intensity by typical LDL reduction; null for non-statin products
    CASE
        WHEN bnf_chemical_code IN ('0212000B0', '0212000AA') THEN 'HIGH_INTENSITY'      -- >=50% LDL reduction
        WHEN bnf_chemical_code IN ('0212000Y0', '0212000X0', '0212000M0') THEN 'MODERATE_INTENSITY'  -- 30-49%
        WHEN lipid_lowering_class = 'STATIN_COMBINATION' THEN 'COMBINATION'
        WHEN lipid_lowering_class = 'STATIN' THEN 'OTHER_STATIN'
    END AS statin_intensity,

    -- Fixed-dose combination products
    bnf_chemical_code IN ('0212000AC', '0212000AJ', '0212000AL') AS is_combination_therapy,

    -- Order recency at build time; order_date is never after the build date
    order_date >= DATEADD(day, -90, CURRENT_DATE()) AS is_recent_3m,
    order_date >= DATEADD(day, -180, CURRENT_DATE()) AS is_recent_6m,
    order_date >= DATEADD(day, -365, CURRENT_DATE()) AS is_recent_12m

FROM classified
