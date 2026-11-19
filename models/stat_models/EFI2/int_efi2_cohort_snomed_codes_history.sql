{#
    append_new_columns adds observation_id and date_recorded on incremental deploy.
    Both columns are populated only for BMI rows and remain null for other deficits.
    Months stored before the columns were added keep nulls until the history full
    refresh backfills BMI rows. Recent months are rebuilt by the incremental window.
#}
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns',
        unique_key='end_date',
        cluster_by=['end_date', 'person_id'],
        snowflake_warehouse=env_var('EFI2_HISTORY_WAREHOUSE', target.warehouse),
        tags=['efi2', 'monthly-full']
    )
}}

-- eFI2 observations valid at each scoring month. Deficits without a published
-- weight are removed before observations are expanded across month-ends.

WITH weighted_deficits AS (
    SELECT DISTINCT LOWER(deficit) AS deficit
    FROM {{ ref('stg_common_efi2_weights') }}
    WHERE weight IS NOT NULL
),

codelist AS (
    SELECT DISTINCT
        cd.snomedct_conceptid::VARCHAR AS snomed_code,
        cd.code_description,
        cd.deficit,
        cd.other_instructions,
        cd.time_constraint_years,
        cd.age_limit
    FROM {{ ref('stg_common_efi2_codelists') }} AS cd
    INNER JOIN weighted_deficits AS wd
        ON LOWER(cd.deficit) = wd.deficit
),

relevant_observations AS (
    -- Match the current cohort's evidence grain before month expansion.
    -- The hash replaces the wider codelist grain fields across month-ends.
    -- Only BMI rows retain observation ID and save time for the selection tie-break.
    SELECT
        CASE WHEN cd.deficit = 'BMI' THEN obs.id END AS observation_id,
        CASE WHEN cd.deficit = 'BMI' THEN obs.date_recorded END AS date_recorded,
        HASH(
            obs.person_id::VARCHAR,
            cd.snomed_code,
            cd.code_description,
            cd.deficit,
            cd.other_instructions,
            cd.time_constraint_years,
            cd.age_limit,
            obs.result_value,
            obs.result_unit_display,
            obs.clinical_effective_date
        ) AS evidence_id,
        obs.person_id,
        cd.snomed_code,
        cd.code_description,
        cd.deficit,
        cd.other_instructions,
        cd.time_constraint_years,
        cd.age_limit,
        obs.result_value,
        obs.result_unit_display AS result_value_unit,
        obs.clinical_effective_date
    FROM {{ ref('stg_olids_observation') }} AS obs
    INNER JOIN codelist AS cd
        ON obs.mapped_concept_code::VARCHAR = cd.snomed_code
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY obs.person_id, cd.snomed_code, cd.code_description,
            cd.deficit, cd.other_instructions, cd.time_constraint_years,
            cd.age_limit, obs.result_value, obs.result_unit_display,
            obs.clinical_effective_date
        ORDER BY obs.date_recorded DESC NULLS LAST, obs.id DESC
    ) = 1
),

patient_months_to_build AS (
    SELECT *
    FROM {{ ref('int_efi2_patient_list_history') }}
    {% if is_incremental() %}
    WHERE {{ rebuild_month_window('end_date') }}
    {% endif %}
)

SELECT
    obs.evidence_id,
    obs.observation_id,
    obs.date_recorded,
    pd.person_id,
    obs.deficit,
    obs.other_instructions,
    obs.result_value,
    obs.clinical_effective_date,
    DATEDIFF('year', pd.date_of_birth, DATE(obs.clinical_effective_date))
        AS age_at_event,
    pd.gender,
    pd.end_date
FROM relevant_observations AS obs
INNER JOIN patient_months_to_build AS pd
    ON obs.person_id = pd.person_id
    AND obs.clinical_effective_date <= pd.end_date
    AND (
        DATEADD('year', obs.time_constraint_years, obs.clinical_effective_date)
            > pd.end_date
        OR obs.time_constraint_years IS NULL
    )
    AND (
        DATEDIFF('year', pd.date_of_birth, DATE(obs.clinical_effective_date))
            >= obs.age_limit
        OR obs.age_limit IS NULL
    )
