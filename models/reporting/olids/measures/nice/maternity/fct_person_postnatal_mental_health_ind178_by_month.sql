{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind178('by_month') }}
