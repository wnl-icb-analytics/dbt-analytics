{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind148('by_month') }}
