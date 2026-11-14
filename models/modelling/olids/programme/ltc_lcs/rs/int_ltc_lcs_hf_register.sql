-- LTC LCS: HF register
-- EMIS source: 'LTC LCS: HF Register*' (R6 shared 11 November 2026). R6 replaces the
-- unexported library item 79888a16 with one inline rule: among HF_COD and HFRES_COD
-- records dated on or before today, excluding Review and Ended episodes, the latest is
-- an HF_COD record. Parent population: currently registered patients.
--
-- Differs from the QOF register (fct_person_heart_failure_register) in two ways: Review
-- and Ended episodes are ignored, and a same-day diagnosis and resolved code are ordered
-- by save time, then observation ID, rather than the diagnosis winning.
-- Records without a clinical date do not qualify. Case-finding bases that still name the
-- library item keep the QOF register as its proxy.

with hf_records as (
    select
        hf.person_id,
        hf.id as observation_id,
        o.clinical_effective_date::date as clinical_date,
        o.date_recorded,
        hf.is_diagnosis_code
    from {{ ref('int_heart_failure_diagnoses_all') }} as hf
    inner join {{ ref('stg_olids_observation') }} as o
        on hf.id = o.id
    where (hf.is_diagnosis_code or hf.is_resolved_code)
        and o.clinical_effective_date::date <= current_date()
        and not exists (
            select 1
            from {{ ref('stg_olids_enriched_concept_map') }} as episode
            where episode.source_concept_id = o.episodicity_source_concept_id
                and episode.source_system in (
                    'EMIS_ObservationEpisode_cs', 'EMIS_Enumeration_cs'
                )
                and upper(episode.source_code) in ('REVIEW', 'ENDED')
        )
),

latest_record as (
    select person_id, is_diagnosis_code
    from hf_records
    qualify row_number() over (
        partition by person_id
        order by clinical_date desc, date_recorded desc nulls last, observation_id desc
    ) = 1
)

select lr.person_id
from latest_record as lr
inner join {{ ref('dim_person_active_patients') }} as a
    on lr.person_id = a.person_id
where lr.is_diagnosis_code
