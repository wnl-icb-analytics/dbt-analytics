{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind123('by_month') }}
