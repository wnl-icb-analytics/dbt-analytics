{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind315('by_month') }}
