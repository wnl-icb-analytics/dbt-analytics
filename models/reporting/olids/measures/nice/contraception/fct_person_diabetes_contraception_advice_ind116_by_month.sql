{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind116('by_month') }}
