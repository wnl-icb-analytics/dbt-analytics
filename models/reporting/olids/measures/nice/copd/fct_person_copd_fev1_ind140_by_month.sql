{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind140('by_month') }}
