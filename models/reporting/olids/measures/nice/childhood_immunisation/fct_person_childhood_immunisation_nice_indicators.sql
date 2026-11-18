{{ config(materialized='table') }}
{{ nice_childhood_immunisation_union('current') }}
