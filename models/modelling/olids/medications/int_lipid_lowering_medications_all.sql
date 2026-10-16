{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All lipid-regulating medication orders (BNF section 2.12): statins, ezetimibe,
bempedoic acid, PCSK9 inhibitors, inclisiran, fibrates, bile acid sequestrants,
nicotinic acid derivatives and omega-3 products.

National STAT_COD, EZETIMIBE_COD, BEMPACID_COD, PCSK9I_COD and INCLISIRAN_COD
lists classify products within BNF 2.12. Other classes use BNF chemical codes.
dm+d ingredient counts identify fixed-dose combinations.

Excludes orders dated before 1990 or after the build date; the source carries both.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH list_membership AS (
    SELECT
        code,
        BOOLOR_AGG(source = 'ECL_CACHE' AND cluster_id = 'STAT_COD') AS is_stat_cod,
        BOOLOR_AGG(source = 'ECL_CACHE' AND cluster_id = 'EZETIMIBE_COD') AS is_ezetimibe_cod,
        BOOLOR_AGG(source = 'ECL_CACHE' AND cluster_id = 'BEMPACID_COD') AS is_bempacid_cod,
        BOOLOR_AGG(source = 'ECL_CACHE' AND cluster_id = 'INCLISIRAN_COD') AS is_inclisiran_cod,
        BOOLOR_AGG(source = 'ECL_CACHE' AND cluster_id = 'PCSK9I_COD') AS is_pcsk9i_cod,
        BOOLOR_AGG(source = 'OPENCODELISTS' AND cluster_id IN (
            'NHS_DRUG_REFSETS/HIATORVASTAT_COD', 'NHS_DRUG_REFSETS/HIROSUVASTAT_COD',
            'NHS_DRUG_REFSETS/HISIMVASTAT_COD')) AS is_high_intensity_statin
    FROM {{ ref('stg_reference_combined_codesets') }}
    WHERE (source = 'ECL_CACHE' AND cluster_id IN (
        'STAT_COD', 'EZETIMIBE_COD', 'BEMPACID_COD', 'INCLISIRAN_COD', 'PCSK9I_COD'))
        OR (source = 'OPENCODELISTS' AND cluster_id IN (
            'NHS_DRUG_REFSETS/HIATORVASTAT_COD', 'NHS_DRUG_REFSETS/HIROSUVASTAT_COD',
            'NHS_DRUG_REFSETS/HISIMVASTAT_COD'))
    GROUP BY code
),
base_orders AS (
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
        bo.*,
        coalesce(lm.is_stat_cod, false) AS is_stat_cod,
        coalesce(lm.is_high_intensity_statin, false) AS national_high_intensity,
        dmd.ingredient_count,
        CASE
            WHEN lm.is_stat_cod AND dmd.ingredient_count >= 2 THEN 'STATIN_COMBINATION'
            WHEN lm.is_stat_cod THEN 'STATIN'
            WHEN lm.is_bempacid_cod THEN 'BEMPEDOIC_ACID'
            WHEN lm.is_ezetimibe_cod THEN 'EZETIMIBE'
            WHEN lm.is_pcsk9i_cod THEN 'PCSK9_INHIBITOR'
            WHEN lm.is_inclisiran_cod THEN 'INCLISIRAN'
            -- No national lists are available for the remaining BNF chemical classes.
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
    FROM base_orders bo
    LEFT JOIN list_membership lm ON bo.mapped_concept_code = lm.code
    LEFT JOIN {{ ref('dmd_product') }} dmd ON bo.mapped_concept_code = dmd.product_code
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
    is_stat_cod AS is_statin,

    -- National high-intensity lists classify product strength, not prescribed daily dose.
    CAST(CASE
        WHEN NOT is_stat_cod THEN NULL
        WHEN national_high_intensity THEN 'HIGH_INTENSITY'
        WHEN lipid_lowering_class = 'STATIN_COMBINATION' THEN 'COMBINATION'
        ELSE 'OTHER_STATIN'
    END AS VARCHAR) AS statin_intensity,

    -- Omega-3 ethyl esters are one agent despite having several dm+d substances.
    COALESCE(ingredient_count >= 2, FALSE) AND lipid_lowering_class <> 'OMEGA_3' AS is_combination_therapy,

    -- Order recency at build time; order_date is never after the build date
    {{ medication_date_flags('order_date', include_future_flag=false) }},
    -- NICE NG238 high intensity means >40% LDL-C reduction; refsets classify product strength.
    national_high_intensity AS is_high_intensity_statin

FROM classified
