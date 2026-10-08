{{
    config(
        materialized='table',
        tags=['fa_checks'])
}}

/* Generate monthly submission timetable for contract activity taking into account bank holidays 
and moving dates to following working day if 22nd of the month falls on a Friday */
WITH bank_holidays AS (
    /*
        England and Wales bank holidays that could affect
        the generated submission dates.
        Add future bank holidays here as the model expands.
    */
    SELECT column1::DATE AS bank_holiday
    FROM VALUES
        ('2026-05-04'),
        ('2026-05-25'),
        ('2026-08-31'),
        ('2026-12-25'),
        ('2026-12-28'),
        ('2027-01-01'),
        ('2027-03-26'),
        ('2027-03-29'),
        ('2027-05-03'),
        ('2027-05-31'),
        ('2027-08-30'),
        ('2027-12-27'),
        ('2027-12-28'),
        ('2028-01-03'),
        ('2028-04-14'),
        ('2028-04-17'),
        ('2028-05-01'),
        ('2028-05-29'),
        ('2028-08-28'),
        ('2028-12-25'),
        ('2028-12-26')
),
activity_months AS (
        --Generates monthly rows from April 2026. ROWCOUNT => 24 covers: FY 2026/27 and FY 2027/28
    SELECT
        DATEADD(
            MONTH,
            SEQ4(),
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
weekend_adjusted AS (
    SELECT
        activity_month,
        target_date,
        CASE
                --Friday rolls forward to Monday.
            WHEN DAYOFWEEKISO(target_date) = 5
                THEN DATEADD(DAY, 3, target_date)
                --Saturday rolls forward to Monday.
            WHEN DAYOFWEEKISO(target_date) = 6
                THEN DATEADD(DAY, 2, target_date)
                --Sunday rolls forward to Monday.
            WHEN DAYOFWEEKISO(target_date) = 7
                THEN DATEADD(DAY, 1, target_date)
            --Monday to Thursday remain unchanged.
            ELSE target_date
        END AS provisional_submission_date
    FROM target_dates
),
submission_dates AS (
    SELECT
        activity_month,
        /*
            If the provisional date is a bank holiday, move to the following day.
            The second check supports consecutive holidays such as Christmas and Boxing Day substitute days.
        */
        CASE
            WHEN bh1.bank_holiday IS NOT NULL
                 AND bh2.bank_holiday IS NOT NULL
                THEN DATEADD(
                    DAY,
                    2,
                    provisional_submission_date
                )
            WHEN bh1.bank_holiday IS NOT NULL
                THEN DATEADD(
                    DAY,
                    1,
                    provisional_submission_date
                )
            ELSE provisional_submission_date
        END AS submission_date
    FROM weekend_adjusted wa
    LEFT JOIN bank_holidays bh1
        ON wa.provisional_submission_date = bh1.bank_holiday
    LEFT JOIN bank_holidays bh2
        ON DATEADD(
            DAY,
            1,
            wa.provisional_submission_date
        ) = bh2.bank_holiday
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
    mod(month(activity_month) + 8, 12) + 1 as fy_submission_month_number,
    TO_CHAR(activity_month,'YYYY-MM') AS activity_year_month,
    TO_CHAR(submission_date,'YYYY-MM') AS submission_month,
       submission_date,
FROM submission_dates
ORDER BY activity_month;



