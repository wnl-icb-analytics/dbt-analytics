{{
    config(
        materialized='table',
        tags=['fa_checks'])
}}

/* Generate monthly submission timetable for contract activity taking into account bank holidays 
and moving dates to following working day if 22nd of the month falls on a Friday 
--10th November use existing bank holiday date table -- and fix this issue 'in weekend_adjusted + submission_dates: after moving a date because of a bank holiday, script does not recheck whether the resulting date is a weekend or another bank holiday.'
--19th November fix. Activity months and Candidate date CTEs are now explicit, deterministic row numbering and day offsets to explicitly control the numbers used to create the months and days 
rather than relying directly on Snowflake's sequence generator 'SEQ4()' which was returning an error when running inside dbt but not in Snowflake.*/
WITH bank_holidays AS (
    --England and Wales bank holidays dictionary used to determine valid submission dates.
    SELECT
        full_date::DATE AS bank_holiday
    FROM {{ ref('stg_dictionary_dbo_datebankholiday') }}
    --FROM DEV__STAGING.DICTIONARY.STG_DICTIONARY_DBO_DATEBANKHOLIDAY
    WHERE in_england_and_wales = TRUE AND YEAR(full_date) BETWEEN 2026 AND 2030
),
activity_months AS (
        --Generates monthly rows from April 2026. ROWCOUNT => 24 covers: FY 2026/27 and FY 2027/28 -- FOR NOW
        --Use the sequence to establish an order - NOT DATEADD which can be unstable, then explicitly number the resulting rows from 1 to 24.
     SELECT
        DATEADD(
            MONTH,
            ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1,
            '2026-04-01'::DATE
        ) AS activity_month
    FROM TABLE(GENERATOR(ROWCOUNT => 24))
),
target_dates AS (
    SELECT
        activity_month,
        CASE
            --November activity:  fixed at 21 December to remain before Christmas.
            WHEN MONTH(activity_month) = 11
                THEN DATE_FROM_PARTS(
                    YEAR(activity_month),
                    12,
                    21
                )
                --Normal deadline: 22nd of the following month.
            ELSE DATE_FROM_PARTS(
                YEAR(DATEADD(MONTH, 1, activity_month)),
                MONTH(DATEADD(MONTH, 1, activity_month)),
                22
            )
        END AS target_date
    FROM activity_months
),
candidate_dates AS (
    --For every target submission date, create 10 possible dates: the target date itself and the following nine days. 
    --It gives the later submission_dates CTE a little 10-day window to search for an allowable submission date.
    SELECT
        td.activity_month,
        td.target_date,
        DATEADD(DAY, g.day_offset, td.target_date) AS candidate_date
    FROM target_dates td
    CROSS JOIN (
        SELECT column1 AS day_offset
        FROM VALUES
            (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)
    ) g
),
submission_dates AS (
    /*   Select the first valid submission date:
        - Monday to Friday
        - not an England and Wales bank holiday
        - if the original target date is Friday, move forward  */
    SELECT
        cd.activity_month,
        MIN(cd.candidate_date) AS submission_date
    FROM candidate_dates cd
    LEFT JOIN bank_holidays bh
        ON cd.candidate_date = bh.bank_holiday
    WHERE DAYOFWEEKISO(cd.candidate_date) BETWEEN 1 AND 5
      AND bh.bank_holiday IS NULL
      AND NOT (
          cd.candidate_date = cd.target_date
          AND DAYOFWEEKISO(cd.target_date) = 5
      )
    GROUP BY cd.activity_month
)

SELECT
    CASE
    WHEN MONTH(activity_month) >= 4
        THEN CONCAT(RIGHT(YEAR(activity_month)::VARCHAR, 2),
            '/',RIGHT((YEAR(activity_month) + 1)::VARCHAR, 2) )
    ELSE CONCAT(RIGHT((YEAR(activity_month) - 1)::VARCHAR, 2),
            '/',RIGHT(YEAR(activity_month)::VARCHAR, 2))
END AS financial_year,
    activity_month,
    TO_CHAR(activity_month,'YYYY-MM') AS activity_year_month,
    mod(month(activity_month) + 8, 12) + 1 as fy_submission_month_number,
    TO_NUMBER(YEAR(activity_month)) AS submission_year,
    TO_CHAR(submission_date,'YYYY-MM') AS submission_month,
       submission_date,
FROM submission_dates
ORDER BY activity_month