{{ config(materialized='table') }}

{{ nice_fh_assessment_union('current') }}
