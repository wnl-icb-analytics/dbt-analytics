{{
    config(
        description="Raw layer (C-LTCS tables). 1:1 passthrough with cleaned column names. \nSource: {{ 'PUBLISHED_REPORTING__DIRECT_CARE' if target.name in ['prod', 'snowflake-prod', 'ci-prod'] else ('DEV__PUBLISHED_REPORTING__DIRECT_CARE' if target.name in ['dev', 'snowflake-dev', 'ci-dev'] else target.name | upper | trim ~ '__PUBLISHED_REPORTING__DIRECT_CARE') }}.C_LTCS.OP_OE_RATIO \ndbt: source(''c_ltcs'', ''OP_OE_RATIO'') \nColumns:\n  PATIENT_ID -> patient_id\n  OE_RATIO -> oe_ratio\n  PREDICTED -> predicted\n  OP_ATT_TOT_12MO -> op_att_tot_12_mo"
    )
}}
select
    "PATIENT_ID" as patient_id,
    "OE_RATIO" as oe_ratio,
    "PREDICTED" as predicted,
    "OP_ATT_TOT_12MO" as op_att_tot_12_mo
from {{ source('c_ltcs', 'OP_OE_RATIO') }}
