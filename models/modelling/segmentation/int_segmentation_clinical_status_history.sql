{{
    config(
        materialized='table',
        cluster_by=['end_date', 'person_id'],
        tags=['monthly-full']
    )
}}

-- Non-activity clinical criteria used by adult and child segmentation.

WITH frailty_latest AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        f.clinical_effective_date AS latest_frailty_date,
        f.frailty_severity AS latest_frailty_severity
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN {{ ref('int_frailty_diagnoses_all') }} AS f
        ON pm.person_id = f.person_id
        AND f.clinical_effective_date <= pm.month_end_date
        AND (
            f.date_recorded IS NULL
            OR CAST(f.date_recorded AS DATE) <= pm.month_end_date
        )
    WHERE pm.is_active
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pm.person_id, pm.month_end_date
        ORDER BY f.clinical_effective_date DESC, f.id DESC
    ) = 1
),

frailty_qualifying AS (
    SELECT *
    FROM frailty_latest
    WHERE latest_frailty_severity IN ('Moderate', 'Severe')
),

palliative_state AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        MAX(IFF(
            p.is_palliative_care_code
                AND p.clinical_effective_date >= '2008-04-01',
            p.clinical_effective_date,
            NULL
        )) AS latest_palliative_care_date,
        MAX(IFF(
            p.is_palliative_care_not_indicated_code,
            p.clinical_effective_date,
            NULL
        )) AS latest_palliative_no_longer_indicated_date
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN {{ ref('int_palliative_care_diagnoses_all') }} AS p
        ON pm.person_id = p.person_id
        AND p.clinical_effective_date <= pm.month_end_date
        AND (
            p.date_recorded IS NULL
            OR CAST(p.date_recorded AS DATE) <= pm.month_end_date
        )
    WHERE pm.is_active
    GROUP BY pm.person_id, pm.month_end_date
),

palliative_qualifying AS (
    SELECT *
    FROM palliative_state
    WHERE
        latest_palliative_care_date IS NOT NULL
        AND (
            latest_palliative_no_longer_indicated_date IS NULL
            OR latest_palliative_no_longer_indicated_date
                <= latest_palliative_care_date
        )
),

residential_events AS (
    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.date_recorded,
        BOOLOR_AGG(obs.cluster_id = 'HOMELESS_COD') AS is_homeless_status
    FROM ({{ get_observations("'RESIDE_COD', 'HOMELESS_COD'") }}) AS obs
    WHERE obs.clinical_effective_date IS NOT NULL
    GROUP BY
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.date_recorded
),

residential_latest AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        r.clinical_effective_date AS latest_residential_status_date,
        r.is_homeless_status
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN residential_events AS r
        ON pm.person_id = r.person_id
        AND r.clinical_effective_date <= pm.month_end_date
        AND (
            r.date_recorded IS NULL
            OR CAST(r.date_recorded AS DATE) <= pm.month_end_date
        )
    WHERE pm.is_active
    -- A homelessness code wins a same-day tie with another residential code
    -- (UKHSA HOMELESS_DAT >= RESIDE_DAT), matching dim_person_homeless.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pm.person_id, pm.month_end_date
        ORDER BY
            CAST(r.clinical_effective_date AS DATE) DESC,
            r.is_homeless_status DESC,
            r.clinical_effective_date DESC,
            r.id DESC
    ) = 1
),

chip_months AS (
    SELECT
        person_id,
        month_end_date AS end_date
    FROM {{ ref('int_segmentation_person_month_spine') }}
    WHERE is_active AND practice_code = 'Y02674'
),

homeless_residential AS (
    SELECT
        person_id,
        end_date,
        latest_residential_status_date
    FROM residential_latest
    WHERE is_homeless_status
),

homeless_keys AS (
    SELECT person_id, end_date
    FROM homeless_residential

    UNION

    SELECT person_id, end_date
    FROM chip_months
),

-- The date is only carried where the latest residential code is a
-- homelessness code, so it never contradicts the flag beside it.
homeless_qualifying AS (
    SELECT
        k.person_id,
        k.end_date,
        r.latest_residential_status_date,
        c.person_id IS NOT NULL AS is_registered_chip
    FROM homeless_keys AS k
    LEFT JOIN homeless_residential AS r
        ON k.person_id = r.person_id
        AND k.end_date = r.end_date
    LEFT JOIN chip_months AS c
        ON k.person_id = c.person_id
        AND k.end_date = c.end_date
),

-- int_alcohol_misuse_disorders owns the age 16 recording threshold.
alcohol_qualifying AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        MAX(a.clinical_effective_date) AS latest_alcohol_disorder_date
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN {{ ref('int_alcohol_misuse_disorders') }} AS a
        ON pm.person_id = a.person_id
        AND a.clinical_effective_date <= pm.month_end_date
        AND (
            a.date_recorded IS NULL
            OR CAST(a.date_recorded AS DATE) <= pm.month_end_date
        )
    WHERE pm.is_active
    GROUP BY pm.person_id, pm.month_end_date
),

substance_events AS (
    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.date_recorded,
        COALESCE(s.status, 'QUALIFYING') AS status
    FROM ({{ get_observations("'ILLSUB_COD'") }}) AS obs
    LEFT JOIN {{ ref('substance_misuse_illsub_status') }} AS s
        ON obs.mapped_concept_code = s.snomed_code
    WHERE obs.clinical_effective_date IS NOT NULL
),

substance_latest AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        s.clinical_effective_date AS latest_substance_misuse_date,
        s.status AS latest_substance_status
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN substance_events AS s
        ON pm.person_id = s.person_id
        AND s.clinical_effective_date <= pm.month_end_date
        AND (
            s.date_recorded IS NULL
            OR CAST(s.date_recorded AS DATE) <= pm.month_end_date
        )
    WHERE pm.is_active
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pm.person_id, pm.month_end_date
        ORDER BY
            s.clinical_effective_date DESC,
            IFF(s.status = 'QUALIFYING', 0, 1),
            s.id DESC
    ) = 1
),

substance_qualifying AS (
    SELECT *
    FROM substance_latest
    WHERE latest_substance_status = 'QUALIFYING'
),

housebound_events AS (
    SELECT
        id,
        person_id,
        clinical_effective_date,
        date_recorded,
        source_cluster_id = 'HOUSEBOUND' AS is_housebound_status
    FROM {{ ref('int_housebound_status_all') }}
),

housebound_latest AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        h.clinical_effective_date AS latest_housebound_status_date,
        h.is_housebound_status
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN housebound_events AS h
        ON pm.person_id = h.person_id
        AND h.clinical_effective_date <= pm.month_end_date
        AND (
            h.date_recorded IS NULL
            OR CAST(h.date_recorded AS DATE) <= pm.month_end_date
        )
    WHERE pm.is_active
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pm.person_id, pm.month_end_date
        ORDER BY h.clinical_effective_date DESC, h.id DESC
    ) = 1
),

housebound_qualifying AS (
    SELECT *
    FROM housebound_latest
    WHERE is_housebound_status
),

child_diagnosis_qualifying AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        COUNT(DISTINCT o.mapped_concept_code) AS complexity_diagnosis_codes,
        MAX(o.clinical_effective_date) AS latest_complexity_diagnosis_date
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN {{ ref('stg_olids_observation') }} AS o
        ON pm.person_id = o.person_id
        AND o.clinical_effective_date <= pm.month_end_date
        AND (
            o.date_recorded IS NULL
            OR CAST(o.date_recorded AS DATE) <= pm.month_end_date
        )
    INNER JOIN {{ ref('children_complexity_diagnosis_codes') }} AS c
        ON o.mapped_concept_code = c.snomed_code
    WHERE pm.is_active AND pm.age < 18
    GROUP BY pm.person_id, pm.month_end_date
),

qualifying_keys AS (
    SELECT person_id, end_date FROM frailty_qualifying
    UNION
    SELECT person_id, end_date FROM palliative_qualifying
    UNION
    SELECT person_id, end_date FROM homeless_qualifying
    UNION
    SELECT person_id, end_date FROM alcohol_qualifying
    UNION
    SELECT person_id, end_date FROM substance_qualifying
    UNION
    SELECT person_id, end_date FROM housebound_qualifying
    UNION
    SELECT person_id, end_date FROM child_diagnosis_qualifying
)

SELECT
    k.person_id,
    k.end_date,

    f.person_id IS NOT NULL AS has_coded_moderate_severe_frailty,
    f.latest_frailty_severity,
    f.latest_frailty_date,

    p.person_id IS NOT NULL AS is_on_palliative_care_register,
    p.latest_palliative_care_date,
    p.latest_palliative_no_longer_indicated_date,

    h.person_id IS NOT NULL AS is_homeless,
    h.latest_residential_status_date,
    COALESCE(h.is_registered_chip, FALSE) AS is_registered_chip,

    a.person_id IS NOT NULL AS has_alcohol_misuse,
    a.latest_alcohol_disorder_date,

    s.person_id IS NOT NULL AS has_substance_misuse,
    s.latest_substance_misuse_date,

    hb.person_id IS NOT NULL AS is_housebound,
    hb.latest_housebound_status_date,

    c.person_id IS NOT NULL AS has_child_complexity_diagnosis,
    c.complexity_diagnosis_codes,
    c.latest_complexity_diagnosis_date
FROM qualifying_keys AS k
LEFT JOIN frailty_qualifying AS f
    ON k.person_id = f.person_id AND k.end_date = f.end_date
LEFT JOIN palliative_qualifying AS p
    ON k.person_id = p.person_id AND k.end_date = p.end_date
LEFT JOIN homeless_qualifying AS h
    ON k.person_id = h.person_id AND k.end_date = h.end_date
LEFT JOIN alcohol_qualifying AS a
    ON k.person_id = a.person_id AND k.end_date = a.end_date
LEFT JOIN substance_qualifying AS s
    ON k.person_id = s.person_id AND k.end_date = s.end_date
LEFT JOIN housebound_qualifying AS hb
    ON k.person_id = hb.person_id AND k.end_date = hb.end_date
LEFT JOIN child_diagnosis_qualifying AS c
    ON k.person_id = c.person_id AND k.end_date = c.end_date
