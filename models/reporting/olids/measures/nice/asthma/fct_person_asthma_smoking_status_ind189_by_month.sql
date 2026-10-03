{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind189('by_month') }}
