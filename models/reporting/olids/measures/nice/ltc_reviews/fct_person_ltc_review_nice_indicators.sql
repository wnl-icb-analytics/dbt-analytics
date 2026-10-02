{{ config(materialized='table') }}
{{ nice_ltc_reviews_union('current') }}
