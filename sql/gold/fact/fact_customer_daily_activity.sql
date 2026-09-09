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
-- 6. Number of support tickets raised
-- ===========================================================


WITH


-- ===========================================================
-- CUSTOMER DATE SPINE
--
-- Creates one row for every customer for every date from
-- registration date up to the current date.
-- ===========================================================

customer_dates AS
(
    SELECT
        c.customer_id,
        c.customer_no,
        d.date_key,
        d.full_date

    FROM silver.customer c

    JOIN gold.dim_date d
        ON d.full_date >= COALESCE(
            c.registration_date,
            DATE '2023-01-01'
        )

       AND d.full_date <= CURRENT_DATE
),


-- ===========================================================
-- STREAMING ACTIVITY
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
        AND status = 'Completed'

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

    COALESCE(
        sa.stream_session_count,
        0
    ),

    COALESCE(
        sa.total_stream_duration_mins,
        0
    ),

    COALESCE(
        ra.physical_rental_count,
        0
    ),

    COALESCE(
        rta.returned_item_count,
        0
    ),

    COALESCE(
        pa.total_amount_spent,
        0
    ),

    COALESCE(
        sta.support_ticket_count,
        0
    )

FROM customer_dates cd


-- ===========================================================
-- CUSTOMER DIMENSION LOOKUP
--
-- Retrieves the customer surrogate key that was valid
-- on the activity date using SCD Type 2 effective dates.
-- ===========================================================

JOIN gold.dim_customer dc
    ON dc.customer_no = cd.customer_no
   AND cd.full_date >= dc.effective_from::date
   AND (cd.full_date < dc.effective_to::date OR dc.effective_to IS NULL)


-- ===========================================================
-- STREAMING
-- ===========================================================

LEFT JOIN streaming_agg sa

    ON sa.customer_id = cd.customer_id

   AND sa.activity_date = cd.full_date


-- ===========================================================
-- RENTALS
-- ===========================================================

LEFT JOIN rental_agg ra

    ON ra.customer_id = cd.customer_id

   AND ra.activity_date = cd.full_date


-- ===========================================================
-- RETURNS
-- ===========================================================

LEFT JOIN return_agg rta

    ON rta.customer_id = cd.customer_id

   AND rta.activity_date = cd.full_date


-- ===========================================================
-- PAYMENTS
-- ===========================================================

LEFT JOIN payment_agg pa

    ON pa.customer_id = cd.customer_id

   AND pa.activity_date = cd.full_date


-- ===========================================================
-- SUPPORT
-- ===========================================================

LEFT JOIN support_agg sta

    ON sta.customer_id = cd.customer_id

   AND sta.activity_date = cd.full_date


-- ===========================================================
-- IDEMPOTENCY
-- ===========================================================

ON CONFLICT
(
    customer_sk,
    date_key
)

DO UPDATE

SET
    stream_session_count =
        EXCLUDED.stream_session_count,

    total_stream_duration_mins =
        EXCLUDED.total_stream_duration_mins,

    physical_rental_count =
        EXCLUDED.physical_rental_count,

    returned_item_count =
        EXCLUDED.returned_item_count,

    total_amount_spent =
        EXCLUDED.total_amount_spent,

    support_ticket_count =
        EXCLUDED.support_ticket_count

RETURNING (xmax = 0) AS is_insert;