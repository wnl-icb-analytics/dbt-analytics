-- Complex adults cohort (v2.2): criterion breakdown by borough of registration.
--
-- Answers "what is driving the cohort numbers, split by criterion and borough".
-- Returns one row per borough per criterion, plus an NCL total row.
--
-- Usage: compile with dbt, then run the compiled queries in Snowflake.
--
-- REFRESH FIRST
--   Dev tables move. The activity windows anchor to CURRENT_DATE (ED, non-
--   elective) or to the latest activity date in the source (GP, outpatients),
--   so figures drift between builds. Rebuild the cohort and its upstreams in
--   ONE run before quoting anything:
--     dbt build --select +fct_person_segment +fct_complex_adults_ed_attendances +fct_complex_adults_nel_admissions
--   That is ~350 models and takes about 10 minutes. Selecting only the
--   segmentation models leaves them sitting on whatever date the OLIDS
--   registers were last built, which is the usual cause of numbers that do
--   not reconcile.
--
-- COLUMNS
--   people          cohort members in that borough meeting the criterion
--   per_1000_adults people per 1,000 currently registered adults in the borough
--   pct_of_cohort   people as a share of the borough cohort
--   sole_reason     people for whom this is the ONLY criterion met in its limb,
--                   i.e. how many would leave the cohort if it were dropped.
--                   This is the marginal-contribution measure - use it, not
--                   people, to answer "how much does this criterion matter".
--   index_vs_ncl    borough rate as an index of the NCL rate (100 = NCL average)
--
-- READING IT
--   Cohort membership needs one complexity criterion AND one activity
--   criterion. Rows overlap and do not sum to the cohort: most people meet
--   several criteria in each limb. Only sole_reason is additive-ish.
--
--   Denominator is currently registered adults (18+), not ONS resident
--   population, so rates are not comparable to published population rates.
--
--   NCL boroughs only. NWL comes through the WSIC route, not this pipeline.
--
-- RELATED
--   Cohort headline and complexity depth: second query at the bottom of this
--   file. Criteria definitions and their sources:
--   models/reporting/segmentation/complex_adults/fct_person_complex_adults.yml

WITH cohort AS (
    SELECT
        borough_registered AS borough,
        complexity_criteria_count,
        has_3plus_ltcs,
        has_moderate_severe_frailty,
        has_alcohol_misuse,
        has_substance_misuse,
        is_on_palliative_care_register,
        is_homeless,
        has_high_acute_use_no_gp,
        ed_attendances_12mo >= 3 AS act_ed,
        nel_admissions_12mo >= 2 AS act_nel,
        gp_appointments_12mo >= 15 AS act_gp,
        outpatient_specialties_12mo >= 5 AS act_op,
        is_housebound AS act_housebound
    FROM {{ ref('fct_person_complex_adults') }}
    WHERE is_active
),

-- The model publishes complexity_criteria_count but no activity equivalent,
-- so derive it here to measure sole reasons the same way on both limbs.
cohort_counted AS (
    SELECT
        *,
        (
            act_ed::INT + act_nel::INT + act_gp::INT
            + act_op::INT + act_housebound::INT
        ) AS activity_criteria_count
    FROM cohort
),

-- Fan each person out to one row per criterion met. Adding or removing a
-- criterion means editing this list and nothing else.
reasons AS (
    SELECT
        c.borough,
        f.value:ord::INT AS ord,
        f.value:limb::STRING AS limb,
        f.value:reason::STRING AS reason,
        f.value:sole::BOOLEAN AS is_sole
    FROM cohort_counted AS c,
        LATERAL FLATTEN(input => ARRAY_COMPACT(ARRAY_CONSTRUCT(
            IFF(c.has_3plus_ltcs, OBJECT_CONSTRUCT(
                'ord', 1, 'limb', 'Complexity',
                'reason', '3+ long-term conditions',
                'sole', c.complexity_criteria_count = 1), NULL),
            IFF(c.has_moderate_severe_frailty, OBJECT_CONSTRUCT(
                'ord', 2, 'limb', 'Complexity',
                'reason', 'Moderate or severe frailty',
                'sole', c.complexity_criteria_count = 1), NULL),
            IFF(c.has_alcohol_misuse, OBJECT_CONSTRUCT(
                'ord', 3, 'limb', 'Complexity',
                'reason', 'Alcohol misuse',
                'sole', c.complexity_criteria_count = 1), NULL),
            IFF(c.has_substance_misuse, OBJECT_CONSTRUCT(
                'ord', 4, 'limb', 'Complexity',
                'reason', 'Substance misuse',
                'sole', c.complexity_criteria_count = 1), NULL),
            IFF(c.has_high_acute_use_no_gp, OBJECT_CONSTRUCT(
                'ord', 5, 'limb', 'Complexity',
                'reason', 'High acute use, no GP contact',
                'sole', c.complexity_criteria_count = 1), NULL),
            IFF(c.is_on_palliative_care_register, OBJECT_CONSTRUCT(
                'ord', 6, 'limb', 'Complexity',
                'reason', 'On palliative care register',
                'sole', c.complexity_criteria_count = 1), NULL),
            IFF(c.is_homeless, OBJECT_CONSTRUCT(
                'ord', 7, 'limb', 'Complexity',
                'reason', 'Homeless',
                'sole', c.complexity_criteria_count = 1), NULL),
            IFF(c.act_gp, OBJECT_CONSTRUCT(
                'ord', 8, 'limb', 'Activity',
                'reason', '15+ GP appointments',
                'sole', c.activity_criteria_count = 1), NULL),
            IFF(c.act_ed, OBJECT_CONSTRUCT(
                'ord', 9, 'limb', 'Activity',
                'reason', '3+ ED attendances',
                'sole', c.activity_criteria_count = 1), NULL),
            IFF(c.act_op, OBJECT_CONSTRUCT(
                'ord', 10, 'limb', 'Activity',
                'reason', '5+ outpatient specialties',
                'sole', c.activity_criteria_count = 1), NULL),
            IFF(c.act_housebound, OBJECT_CONSTRUCT(
                'ord', 11, 'limb', 'Activity',
                'reason', 'Housebound',
                'sole', c.activity_criteria_count = 1), NULL),
            IFF(c.act_nel, OBJECT_CONSTRUCT(
                'ord', 12, 'limb', 'Activity',
                'reason', '2+ non-elective admissions',
                'sole', c.activity_criteria_count = 1), NULL)
        ))) AS f
),

-- Borough rows plus an NCL total row.
by_area AS (
    SELECT
        IFF(
            GROUPING(borough) = 1,
            'NCL',
            COALESCE(borough, 'Unknown')
        ) AS area,
        ord, limb, reason,
        COUNT(*) AS people,
        COUNT_IF(is_sole) AS sole_reason
    FROM reasons
    GROUP BY GROUPING SETS (
        (borough, ord, limb, reason),
        (ord, limb, reason)
    )
),

denominator AS (
    SELECT
        IFF(
            GROUPING(borough_registered) = 1,
            'NCL',
            COALESCE(borough_registered, 'Unknown')
        ) AS area,
        COUNT(*) AS adults
    FROM {{ ref('dim_person_demographics') }}
    WHERE is_active AND age >= 18
    GROUP BY ROLLUP(borough_registered)
),

cohort_size AS (
    SELECT
        IFF(
            GROUPING(borough) = 1,
            'NCL',
            COALESCE(borough, 'Unknown')
        ) AS area,
        COUNT(*) AS cohort
    FROM cohort
    GROUP BY ROLLUP(borough)
),

ncl_rate AS (
    SELECT b.ord, b.people * 1000.0 / d.adults AS per_1000
    FROM by_area AS b
    INNER JOIN denominator AS d ON b.area = d.area
    WHERE b.area = 'NCL'
)

SELECT
    b.area AS borough,
    b.limb,
    b.reason,
    b.people,
    ROUND(b.people * 1000.0 / d.adults, 1) AS per_1000_adults,
    ROUND(b.people * 100.0 / cs.cohort, 1) AS pct_of_cohort,
    b.sole_reason,
    ROUND(b.sole_reason * 100.0 / cs.cohort, 1) AS pct_of_cohort_sole,
    ROUND(
        (b.people * 1000.0 / d.adults) / NULLIF(n.per_1000, 0) * 100
    ) AS index_vs_ncl
FROM by_area AS b
INNER JOIN denominator AS d ON b.area = d.area
INNER JOIN cohort_size AS cs ON b.area = cs.area
INNER JOIN ncl_rate AS n ON b.ord = n.ord
ORDER BY IFF(b.area = 'NCL', 0, 1), b.area, b.ord;


-- Cohort headline by borough, and how deep the complexity runs.
-- complexity_1 counts people meeting exactly one complexity criterion, i.e.
-- the part of the cohort sitting on the threshold.
SELECT
    IFF(
        GROUPING(d.borough_registered) = 1,
        'NCL',
        COALESCE(d.borough_registered, 'Unknown')
    ) AS borough,
    COUNT(*) AS registered_adults,
    COUNT(c.person_id) AS cohort,
    ROUND(COUNT(c.person_id) * 1000.0 / COUNT(*), 1) AS per_1000_adults,
    COUNT_IF(c.complexity_criteria_count = 1) AS complexity_1,
    COUNT_IF(c.complexity_criteria_count >= 3) AS complexity_3plus,
    ROUND(AVG(c.complexity_criteria_count), 2) AS mean_complexity_criteria
FROM {{ ref('dim_person_demographics') }} AS d
LEFT JOIN {{ ref('fct_person_complex_adults') }} AS c
    ON d.person_id = c.person_id
    AND c.is_active
WHERE d.is_active AND d.age >= 18
GROUP BY ROLLUP(d.borough_registered)
ORDER BY IFF(GROUPING(d.borough_registered) = 1, 0, 1), 1;
