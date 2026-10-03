{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind115('by_month') }}
