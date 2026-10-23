{{
    config(
        materialized='view',
        tags=['quick_reports'])
}}
-- FINAL model in publishing layer
SELECT *
FROM {{ ref("int_ecds_quick_12m_rolling_overview") }}
--FROM DEV__MODELLING.ACUTE.INT_ECDS_QUICK_12M_ROLLING_OVERVIEW