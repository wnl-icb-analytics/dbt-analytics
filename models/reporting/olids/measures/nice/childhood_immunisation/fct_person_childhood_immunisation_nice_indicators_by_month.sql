{{ config(materialized='table', tags=['monthly-full', 'nice-history'], cluster_by=['reporting_date', 'person_id']) }}

{{ nice_childhood_immunisation_union('by_month') }}
