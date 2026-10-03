{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind122('by_month') }}
