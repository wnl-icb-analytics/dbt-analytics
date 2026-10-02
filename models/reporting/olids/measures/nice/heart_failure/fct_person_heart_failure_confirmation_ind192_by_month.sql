{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind192('by_month') }}
