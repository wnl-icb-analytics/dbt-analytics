{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND133: https://www.nice.org.uk/indicators/ind133
-- Antiplatelet or oral anticoagulant evidence in 12 months for recorded non-haemorrhagic stroke or TIA.
-- Excludes people contraindicated to all four of salicylates, clopidogrel, dipyridamole and oral anticoagulants (persisting at any time or expiring in 12 months), as NICE lists; QOF resolves the timing of persisting and expiring records.
{{ nice_ind133('by_month') }}
