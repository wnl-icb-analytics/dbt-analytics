-- Complex adults cohort (v2.2): the three tables sent to K Saravanakumar,
-- 13 August 2026. Run each query separately; each reproduces one table.
--
--   Query 1  Cohort by borough
--   Query 2  Criteria per 1,000 adults, boroughs as columns
--   Query 3  NCL people per criterion, and how many depend on it alone
--
-- Usage: compile with dbt, then run the compiled queries in Snowflake.
--
-- REBUILD FIRST. Dev tables move between builds - see the header of
-- complex_adults_criteria_by_borough.sql in this folder for the build command
-- and for the caveats that belong with these numbers (rows overlap, the
-- denominator is registered adults not residents, NCL boroughs only).
--
-- For a long-format version that is easier to pivot or filter, use
-- complex_adults_criteria_by_borough.sql instead. These three exist so the
-- exact tables in the email can be reproduced.
--
-- Sort orders differ between tables because the email's did: table 1 and 2 by
-- rate, table 3 by number of people.


-- ===========================================================================
-- QUERY 1: cohort by borough.
-- ===========================================================================
SELECT
    IFF(
        GROUPING(d.borough_registered) = 1,
        'NCL',
        COALESCE(d.borough_registered, 'Unknown')
    ) AS borough,
    COUNT(*) AS registered_adults,
    COUNT(c.person_id) AS cohort,
    ROUND(COUNT(c.person_id) * 1000.0 / COUNT(*), 1) AS per_1000_adults
FROM {{ ref('dim_person_demographics') }} AS d
LEFT JOIN {{ ref('fct_person_complex_adults') }} AS c
    ON d.person_id = c.person_id
    AND c.is_active
WHERE d.is_active AND d.age >= 18
GROUP BY ROLLUP(d.borough_registered)
-- NCL last, boroughs by rate
ORDER BY IFF(GROUPING(d.borough_registered) = 1, 1, 0), per_1000_adults DESC;


-- ===========================================================================
-- QUERY 2: criteria per 1,000 adults, boroughs as columns.
-- ===========================================================================

WITH cohort AS (
    SELECT
        borough_registered AS borough,
        complexity_criteria_count,
        has_3plus_ltcs, has_moderate_severe_frailty, has_alcohol_misuse,
        has_substance_misuse, is_on_palliative_care_register, is_homeless,
        has_high_acute_use_no_gp,
        ed_attendances_12mo >= 3 AS act_ed,
        nel_admissions_12mo >= 2 AS act_nel,
        gp_appointments_12mo >= 15 AS act_gp,
        outpatient_specialties_12mo >= 5 AS act_op,
        is_housebound AS act_housebound
    FROM {{ ref('fct_person_complex_adults') }}
    WHERE is_active
),

reasons AS (
    SELECT
        c.borough,
        f.value:ord::INT AS ord,
        f.value:reason::STRING AS reason
    FROM cohort AS c,
        LATERAL FLATTEN(input => ARRAY_COMPACT(ARRAY_CONSTRUCT(
            IFF(c.has_3plus_ltcs, OBJECT_CONSTRUCT('ord', 1,
                'reason', 'Complex: 3+ long-term conditions'), NULL),
            IFF(c.has_moderate_severe_frailty, OBJECT_CONSTRUCT('ord', 2,
                'reason', 'Complex: Moderate or severe frailty'), NULL),
            IFF(c.has_alcohol_misuse, OBJECT_CONSTRUCT('ord', 3,
                'reason', 'Complex: Alcohol misuse'), NULL),
            IFF(c.has_substance_misuse, OBJECT_CONSTRUCT('ord', 4,
                'reason', 'Complex: Substance misuse'), NULL),
            IFF(c.has_high_acute_use_no_gp, OBJECT_CONSTRUCT('ord', 5,
                'reason', 'Complex: High acute use, no GP contact'), NULL),
            IFF(c.is_on_palliative_care_register, OBJECT_CONSTRUCT('ord', 6,
                'reason', 'Complex: On palliative care register'), NULL),
            IFF(c.is_homeless, OBJECT_CONSTRUCT('ord', 7,
                'reason', 'Complex: Homeless'), NULL),
            IFF(c.act_gp, OBJECT_CONSTRUCT('ord', 8,
                'reason', 'Activity: 15+ GP appointments'), NULL),
            IFF(c.act_ed, OBJECT_CONSTRUCT('ord', 9,
                'reason', 'Activity: 3+ ED attendances'), NULL),
            IFF(c.act_op, OBJECT_CONSTRUCT('ord', 10,
                'reason', 'Activity: 5+ outpatient specialties'), NULL),
            IFF(c.act_housebound, OBJECT_CONSTRUCT('ord', 11,
                'reason', 'Activity: Housebound'), NULL),
            IFF(c.act_nel, OBJECT_CONSTRUCT('ord', 12,
                'reason', 'Activity: 2+ non-elective admissions'), NULL)
        ))) AS f
),

denominator AS (
    SELECT borough_registered AS borough, COUNT(*) AS adults
    FROM {{ ref('dim_person_demographics') }}
    WHERE is_active AND age >= 18
    GROUP BY 1
),

-- Criterion rows: borough rate, and the NCL rate repeated on every row.
rates AS (
    SELECT
        r.ord,
        r.reason,
        r.borough,
        COUNT(*) * 1000.0 / MAX(d.adults) AS per_1000
    FROM reasons AS r
    INNER JOIN denominator AS d ON r.borough = d.borough
    GROUP BY 1, 2, 3
),

ncl AS (
    SELECT
        ord,
        COUNT(*) * 1000.0 / (SELECT SUM(adults) FROM denominator) AS per_1000
    FROM reasons
    GROUP BY 1
),

-- Total cohort row, same shape.
cohort_rates AS (
    SELECT
        c.borough,
        COUNT(*) * 1000.0 / MAX(d.adults) AS per_1000
    FROM cohort AS c
    INNER JOIN denominator AS d ON c.borough = d.borough
    GROUP BY 1
)

SELECT
    r.ord,
    r.reason,
    ROUND(MAX(IFF(r.borough = 'Barnet', r.per_1000, NULL)), 1) AS barnet,
    ROUND(MAX(IFF(r.borough = 'Camden', r.per_1000, NULL)), 1) AS camden,
    ROUND(MAX(IFF(r.borough = 'Enfield', r.per_1000, NULL)), 1) AS enfield,
    ROUND(MAX(IFF(r.borough = 'Haringey', r.per_1000, NULL)), 1) AS haringey,
    ROUND(MAX(IFF(r.borough = 'Islington', r.per_1000, NULL)), 1) AS islington,
    ROUND(MAX(n.per_1000), 1) AS ncl
FROM rates AS r
INNER JOIN ncl AS n ON r.ord = n.ord
GROUP BY 1, 2

UNION ALL

SELECT
    99,
    'TOTAL COHORT',
    ROUND(MAX(IFF(borough = 'Barnet', per_1000, NULL)), 1),
    ROUND(MAX(IFF(borough = 'Camden', per_1000, NULL)), 1),
    ROUND(MAX(IFF(borough = 'Enfield', per_1000, NULL)), 1),
    ROUND(MAX(IFF(borough = 'Haringey', per_1000, NULL)), 1),
    ROUND(MAX(IFF(borough = 'Islington', per_1000, NULL)), 1),
    ROUND(
        (SELECT COUNT(*) FROM cohort) * 1000.0
        / (SELECT SUM(adults) FROM denominator), 1)
FROM cohort_rates

ORDER BY 1;


-- ===========================================================================
-- QUERY 3: NCL people per criterion, and how many depend on it alone.
-- sole_reason = the criterion is the person's only one in its limb, so they
-- would leave the cohort if it were dropped.
WITH cohort AS (
    SELECT
        complexity_criteria_count,
        has_3plus_ltcs, has_moderate_severe_frailty, has_alcohol_misuse,
        has_substance_misuse, is_on_palliative_care_register, is_homeless,
        has_high_acute_use_no_gp,
        ed_attendances_12mo >= 3 AS act_ed,
        nel_admissions_12mo >= 2 AS act_nel,
        gp_appointments_12mo >= 15 AS act_gp,
        outpatient_specialties_12mo >= 5 AS act_op,
        is_housebound AS act_housebound
    FROM {{ ref('fct_person_complex_adults') }}
    WHERE is_active
),

counted AS (
    SELECT
        *,
        (
            act_ed::INT + act_nel::INT + act_gp::INT
            + act_op::INT + act_housebound::INT
        ) AS activity_criteria_count
    FROM cohort
),

reasons AS (
    SELECT
        f.value:ord::INT AS ord,
        f.value:reason::STRING AS reason,
        f.value:sole::BOOLEAN AS is_sole
    FROM counted AS c,
        LATERAL FLATTEN(input => ARRAY_COMPACT(ARRAY_CONSTRUCT(
            IFF(c.has_3plus_ltcs, OBJECT_CONSTRUCT('ord', 1,
                'reason', 'Complex: 3+ long-term conditions',
                'sole', c.complexity_criteria_count = 1), NULL),
            IFF(c.has_moderate_severe_frailty, OBJECT_CONSTRUCT('ord', 2,
                'reason', 'Complex: Moderate or severe frailty',
                'sole', c.complexity_criteria_count = 1), NULL),
            IFF(c.has_alcohol_misuse, OBJECT_CONSTRUCT('ord', 3,
                'reason', 'Complex: Alcohol misuse',
                'sole', c.complexity_criteria_count = 1), NULL),
            IFF(c.has_high_acute_use_no_gp, OBJECT_CONSTRUCT('ord', 4,
                'reason', 'Complex: High acute use, no GP contact',
                'sole', c.complexity_criteria_count = 1), NULL),
            IFF(c.has_substance_misuse, OBJECT_CONSTRUCT('ord', 5,
                'reason', 'Complex: Substance misuse',
                'sole', c.complexity_criteria_count = 1), NULL),
            IFF(c.is_on_palliative_care_register, OBJECT_CONSTRUCT('ord', 6,
                'reason', 'Complex: On palliative care register',
                'sole', c.complexity_criteria_count = 1), NULL),
            IFF(c.is_homeless, OBJECT_CONSTRUCT('ord', 7,
                'reason', 'Complex: Homeless',
                'sole', c.complexity_criteria_count = 1), NULL),
            IFF(c.act_gp, OBJECT_CONSTRUCT('ord', 8,
                'reason', 'Activity: 15+ GP appointments',
                'sole', c.activity_criteria_count = 1), NULL),
            IFF(c.act_ed, OBJECT_CONSTRUCT('ord', 9,
                'reason', 'Activity: 3+ ED attendances',
                'sole', c.activity_criteria_count = 1), NULL),
            IFF(c.act_op, OBJECT_CONSTRUCT('ord', 10,
                'reason', 'Activity: 5+ outpatient specialties',
                'sole', c.activity_criteria_count = 1), NULL),
            IFF(c.act_housebound, OBJECT_CONSTRUCT('ord', 11,
                'reason', 'Activity: Housebound',
                'sole', c.activity_criteria_count = 1), NULL),
            IFF(c.act_nel, OBJECT_CONSTRUCT('ord', 12,
                'reason', 'Activity: 2+ non-elective admissions',
                'sole', c.activity_criteria_count = 1), NULL)
        ))) AS f
)

SELECT
    reason,
    COUNT(*) AS people,
    COUNT_IF(is_sole) AS sole_reason
FROM reasons
GROUP BY ord, reason
ORDER BY ord;
