{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Person-level substance (illicit drug) misuse status.

Grain: one row per person with any ILLSUB_COD record.

Uses the NHS England Primary Care Domain refset ILLSUB_COD, which is nationally
maintained and diagnosis-led. Cluster membership on its own is not evidence of
current misuse: the refset also carries negation codes ("Does not misuse drugs"),
abstinence codes and in-remission codes. Each code is therefore classified
QUALIFYING or RESOLVING in the substance_misuse_illsub_status seed, and this
model applies latest-record-wins:

    has_substance_misuse = the person's MOST RECENT ILLSUB_COD record is QUALIFYING

so a negation, abstinence or remission code recorded after a dependence code
correctly takes the person out, and an older negation does not cancel a newer
dependence code.

Tie-break on same-date records: QUALIFYING wins. The common real-world case is an
annual review recording e.g. "Abstinent from drug misuse on maintenance
replacement" alongside "Opioid dependence" on the same day — that person is in
treatment and still has the underlying condition, so the qualifying code is the
right read.

No age floor is applied here, unlike the sibling int_alcohol_misuse_disorders
(>= 16). That model's floor exists because alcohol screening codes are recorded
unreliably in paediatrics; ILLSUB_COD is diagnosis-led, where a record in a
younger patient is meaningful. Consumers needing an adult-only population should
apply their own age filter.
*/

WITH classified_observations AS (
    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        -- Codes present in the cluster but absent from the seed default to
        -- QUALIFYING (the refset is diagnosis-led, so misuse is the safer
        -- default). Drift is caught loudly by the seed coverage test rather
        -- than silently changing who qualifies.
        COALESCE(s.status, 'QUALIFYING') AS status,
        s.category

    FROM ({{ get_observations("'ILLSUB_COD'") }}) AS obs
    LEFT JOIN {{ ref('substance_misuse_illsub_status') }} AS s
        ON obs.mapped_concept_code = s.snomed_code
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= CURRENT_DATE()  -- No future dates
),

latest_record AS (
    SELECT
        person_id,
        clinical_effective_date AS latest_record_date,
        concept_code AS latest_concept_code,
        concept_display AS latest_concept_display,
        status AS latest_status,
        category AS latest_category
    FROM classified_observations
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id
        ORDER BY
            clinical_effective_date DESC,
            -- QUALIFYING wins same-date ties (see header)
            CASE WHEN status = 'QUALIFYING' THEN 0 ELSE 1 END,
            id DESC
    ) = 1
),

person_summary AS (
    SELECT
        person_id,
        MIN(CASE WHEN status = 'QUALIFYING' THEN clinical_effective_date END)
            AS earliest_qualifying_date,
        MAX(CASE WHEN status = 'QUALIFYING' THEN clinical_effective_date END)
            AS latest_qualifying_date,
        COUNT(CASE WHEN status = 'QUALIFYING' THEN 1 END) AS qualifying_record_count,
        COUNT(CASE WHEN status = 'RESOLVING' THEN 1 END) AS resolving_record_count,
        COUNT(*) AS total_record_count
    FROM classified_observations
    GROUP BY person_id
)

SELECT
    ps.person_id,

    -- Latest-record-wins outcome
    lr.latest_status = 'QUALIFYING' AS has_substance_misuse,

    -- The record the outcome is based on, for traceability
    lr.latest_record_date,
    lr.latest_concept_code,
    lr.latest_concept_display,
    lr.latest_status,
    lr.latest_category,

    -- Full history
    ps.earliest_qualifying_date,
    ps.latest_qualifying_date,
    ps.qualifying_record_count,
    ps.resolving_record_count,
    ps.total_record_count

FROM person_summary AS ps
INNER JOIN latest_record AS lr
    ON ps.person_id = lr.person_id
