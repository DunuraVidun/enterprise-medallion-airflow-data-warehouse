-- ===========================================================
-- GOLD FACT CUSTOMER DAILY ACTIVITY
--
-- Grain:
-- One row per customer per day
--
-- Measures:
-- 1. Number of streaming sessions
-- 2. Total streaming duration
-- 3. Number of physical rentals
-- 4. Number of returned items
-- 5. Total amount spent
-- 6. Number of support tickets
--
-- SCD2 RULE:
-- The customer dimension version valid at the END of the
-- activity day is used for the fact row.
--
-- FACT TABLE IS FULLY REBUILT ON EACH RUN.
-- ===========================================================


-- ===========================================================
-- STEP 0
-- FULL REFRESH
-- ===========================================================

TRUNCATE TABLE gold.fact_customer_daily_activity;


WITH


-- ===========================================================
-- CUSTOMER DATE SPINE
--
-- One row per customer per date from registration date
-- through the current date.
--
-- If registration_date is unavailable, 2023-01-01 is used
-- as the analytical fallback date.
-- ===========================================================

customer_dates AS
(
    SELECT
        c.customer_id,
        c.customer_no,

        d.date_key,
        d.full_date

    FROM silver.customer AS c

    JOIN gold.dim_date AS d

        ON d.full_date >=
           COALESCE(
               c.registration_date,
               DATE '2023-01-01'
           )

       AND d.full_date <= CURRENT_DATE
),


-- ===========================================================
-- STREAMING ACTIVITY
--
-- Streaming sessions are counted on the date they started.
-- ===========================================================

streaming_agg AS
(
    SELECT
        customer_id,

        start_time::DATE AS activity_date,

        COUNT(stream_id) AS stream_session_count,

        COALESCE(
            SUM(watch_duration),
            0
        ) AS total_stream_duration_mins

    FROM silver.streaming_session

    WHERE start_time IS NOT NULL

    GROUP BY
        customer_id,
        start_time::DATE
),


-- ===========================================================
-- PHYSICAL RENTALS
--
-- Rental is counted on rental_date.
-- ===========================================================

rental_agg AS
(
    SELECT
        customer_id,

        rental_date AS activity_date,

        COUNT(rental_id) AS physical_rental_count

    FROM silver.rental

    WHERE rental_date IS NOT NULL

    GROUP BY
        customer_id,
        rental_date
),


-- ===========================================================
-- RETURNS
--
-- Return is counted on the actual return_date.
-- ===========================================================

return_agg AS
(
    SELECT
        customer_id,

        return_date AS activity_date,

        COUNT(rental_id) AS returned_item_count

    FROM silver.rental

    WHERE return_date IS NOT NULL

    GROUP BY
        customer_id,
        return_date
),


-- ===========================================================
-- CUSTOMER PAYMENTS
--
-- Only completed payments are included.
-- ===========================================================

payment_agg AS
(
    SELECT
        customer_id,

        payment_date::DATE AS activity_date,

        COALESCE(
            SUM(amount),
            0
        ) AS total_amount_spent

    FROM silver.payment

    WHERE payment_date IS NOT NULL

      AND UPPER(status) = 'COMPLETED'

    GROUP BY
        customer_id,
        payment_date::DATE
),


-- ===========================================================
-- SUPPORT TICKETS
--
-- Ticket is counted on opened_date.
-- ===========================================================

support_agg AS
(
    SELECT
        customer_id,

        opened_date::DATE AS activity_date,

        COUNT(ticket_id) AS support_ticket_count

    FROM silver.support_ticket

    WHERE opened_date IS NOT NULL

    GROUP BY
        customer_id,
        opened_date::DATE
)


-- ===========================================================
-- LOAD FACT TABLE
-- ===========================================================

INSERT INTO gold.fact_customer_daily_activity
(
    customer_sk,
    date_key,

    stream_session_count,
    total_stream_duration_mins,

    physical_rental_count,
    returned_item_count,

    total_amount_spent,

    support_ticket_count
)

SELECT
    dc.customer_sk,

    cd.date_key,


    -- =======================================================
    -- STREAMING
    -- =======================================================

    COALESCE(
        sa.stream_session_count,
        0
    ) AS stream_session_count,

    COALESCE(
        sa.total_stream_duration_mins,
        0
    ) AS total_stream_duration_mins,


    -- =======================================================
    -- RENTALS
    -- =======================================================

    COALESCE(
        ra.physical_rental_count,
        0
    ) AS physical_rental_count,


    -- =======================================================
    -- RETURNS
    -- =======================================================

    COALESCE(
        rta.returned_item_count,
        0
    ) AS returned_item_count,


    -- =======================================================
    -- PAYMENTS
    -- =======================================================

    COALESCE(
        pa.total_amount_spent,
        0
    ) AS total_amount_spent,


    -- =======================================================
    -- SUPPORT
    -- =======================================================

    COALESCE(
        sta.support_ticket_count,
        0
    ) AS support_ticket_count


FROM customer_dates AS cd


-- ===========================================================
-- CUSTOMER DIMENSION SCD2 LOOKUP
--
-- BUSINESS RULE:
-- Use the customer dimension version that was valid at the
-- END of the activity day.
--
-- END OF DAY =
--
-- full_date + 1 day - 1 microsecond
--
-- SCD2 interval:
--
-- effective_from <= end_of_day
-- AND
-- end_of_day < effective_to
--
-- Supports:
-- 1. Closed SCD2 records
-- 2. Current/open-ended records where effective_to IS NULL
-- ===========================================================

JOIN gold.dim_customer AS dc

    ON dc.customer_no = cd.customer_no

   AND
       (
           cd.full_date
           + INTERVAL '1 day'
           - INTERVAL '1 microsecond'
       ) >= dc.effective_from

   AND
       (
           dc.effective_to IS NULL

           OR

           (
               cd.full_date
               + INTERVAL '1 day'
               - INTERVAL '1 microsecond'
           ) < dc.effective_to
       )


-- ===========================================================
-- STREAMING
-- ===========================================================

LEFT JOIN streaming_agg AS sa

    ON sa.customer_id = cd.customer_id

   AND sa.activity_date = cd.full_date


-- ===========================================================
-- RENTALS
-- ===========================================================

LEFT JOIN rental_agg AS ra

    ON ra.customer_id = cd.customer_id

   AND ra.activity_date = cd.full_date


-- ===========================================================
-- RETURNS
-- ===========================================================

LEFT JOIN return_agg AS rta

    ON rta.customer_id = cd.customer_id

   AND rta.activity_date = cd.full_date


-- ===========================================================
-- PAYMENTS
-- ===========================================================

LEFT JOIN payment_agg AS pa

    ON pa.customer_id = cd.customer_id

   AND pa.activity_date = cd.full_date


-- ===========================================================
-- SUPPORT
-- ===========================================================

LEFT JOIN support_agg AS sta

    ON sta.customer_id = cd.customer_id

   AND sta.activity_date = cd.full_date;


-- ===========================================================
-- END
-- ===========================================================