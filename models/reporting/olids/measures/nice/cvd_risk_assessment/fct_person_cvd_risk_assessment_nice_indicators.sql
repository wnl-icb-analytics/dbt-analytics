{{ config(materialized='table') }}

{{ nice_cvd_risk_assessment_union('current') }}
