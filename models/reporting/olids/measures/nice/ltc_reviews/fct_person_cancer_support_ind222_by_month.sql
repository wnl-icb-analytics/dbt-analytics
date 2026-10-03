{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind222('by_month') }}
