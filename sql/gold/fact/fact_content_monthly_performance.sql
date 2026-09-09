-- ===========================================================
-- GOLD FACT CONTENT MONTHLY PERFORMANCE
--
-- Grain:
-- One row per content item per month
--
-- Measures:
-- 1. Total streams
-- 2. Total rental count
-- 3. Revenue generated
-- 4. Average customer rating
-- 5. Number of wishlist additions
-- ===========================================================


WITH


-- ===========================================================
-- CONTENT MONTH SPINE
--
-- One row for every content item for every month from
-- release month up to current month.
--
-- The date_key represents the first day of the month.
-- ===========================================================

content_months AS
(
    SELECT
        c.content_id,

        d.date_key,

        d.full_date AS month_start

    FROM silver.content c

    JOIN gold.dim_date d

        ON d.day = 1

       AND d.full_date <=
           DATE_TRUNC(
               'month',
               CURRENT_DATE
           )::DATE

       AND d.full_date >= GREATEST
       (
           COALESCE(
               DATE_TRUNC(
                   'month',
                   c.release_date
               )::DATE,

               DATE '2023-01-01'
           ),

           DATE '2023-01-01'
       )
),


-- ===========================================================
-- STREAMING PERFORMANCE
-- ===========================================================

stream_agg AS
(
    SELECT
        content_id,

        DATE_TRUNC(
            'month',
            start_time
        )::DATE AS month_start,

        COUNT(stream_id) AS total_streams

    FROM silver.streaming_session

    WHERE start_time IS NOT NULL

    GROUP BY
        content_id,
        DATE_TRUNC(
            'month',
            start_time
        )::DATE
),


-- ===========================================================
-- PHYSICAL RENTALS BY CONTENT
--
-- rental -> inventory_item -> content
-- ===========================================================

rental_agg AS
(
    SELECT
        ii.content_id,

        DATE_TRUNC(
            'month',
            r.rental_date
        )::DATE AS month_start,

        COUNT(r.rental_id) AS total_rental_count

    FROM silver.rental r

    JOIN silver.inventory_item ii

        ON ii.inventory_id = r.inventory_id

    WHERE r.rental_date IS NOT NULL

    GROUP BY
        ii.content_id,

        DATE_TRUNC(
            'month',
            r.rental_date
        )::DATE
),


-- ===========================================================
-- REVENUE BY CONTENT
--
-- Payment is linked to rental.
--
-- payment -> rental -> inventory item -> content
-- ===========================================================

revenue_agg AS
(
    SELECT
        ii.content_id,

        DATE_TRUNC(
            'month',
            p.payment_date
        )::DATE AS month_start,

        COALESCE(
            SUM(p.amount),
            0
        ) AS revenue_generated

    FROM silver.payment p

    JOIN silver.rental r

        ON r.rental_id = p.rental_id

    JOIN silver.inventory_item ii

        ON ii.inventory_id = r.inventory_id

    WHERE p.payment_date IS NOT NULL

      AND p.rental_id IS NOT NULL

    GROUP BY
        ii.content_id,

        DATE_TRUNC(
            'month',
            p.payment_date
        )::DATE
),


-- ===========================================================
-- CUSTOMER RATINGS
-- ===========================================================

rating_agg AS
(
    SELECT
        content_id,

        DATE_TRUNC(
            'month',
            review_date
        )::DATE AS month_start,

        AVG(
            rating::DECIMAL
        ) AS average_customer_rating

    FROM silver.review

    WHERE review_date IS NOT NULL

      AND rating IS NOT NULL

    GROUP BY
        content_id,

        DATE_TRUNC(
            'month',
            review_date
        )::DATE
),


-- ===========================================================
-- WISHLIST ADDITIONS
-- ===========================================================

wishlist_agg AS
(
    SELECT
        content_id,

        DATE_TRUNC(
            'month',
            added_date
        )::DATE AS month_start,

        COUNT(wishlist_id)
            AS wishlist_addition_count

    FROM silver.wishlist

    WHERE added_date IS NOT NULL

    GROUP BY
        content_id,

        DATE_TRUNC(
            'month',
            added_date
        )::DATE
)


-- ===========================================================
-- LOAD FACT TABLE
-- ===========================================================

INSERT INTO gold.fact_content_monthly_performance
(
    content_sk,
    date_key,

    total_streams,
    total_rental_count,

    revenue_generated,

    average_customer_rating,

    wishlist_addition_count
)

SELECT
    dc.content_sk,

    cm.date_key,

    COALESCE(
        sa.total_streams,
        0
    ),

    COALESCE(
        ra.total_rental_count,
        0
    ),

    COALESCE(
        rva.revenue_generated,
        0
    ),

    COALESCE(
        rta.average_customer_rating,
        0
    ),

    COALESCE(
        wa.wishlist_addition_count,
        0
    )

FROM content_months cm


-- ===========================================================
-- CONTENT DIMENSION LOOKUP
-- ===========================================================

JOIN gold.dim_content dc

    ON dc.content_id = cm.content_id

   AND cm.month_start >= dc.effective_from

   AND (
        cm.month_start < dc.effective_to
        OR dc.effective_to IS NULL
       )


-- ===========================================================
-- STREAMING
-- ===========================================================

LEFT JOIN stream_agg sa

    ON sa.content_id = cm.content_id

   AND sa.month_start = cm.month_start


-- ===========================================================
-- RENTALS
-- ===========================================================

LEFT JOIN rental_agg ra

    ON ra.content_id = cm.content_id

   AND ra.month_start = cm.month_start


-- ===========================================================
-- REVENUE
-- ===========================================================

LEFT JOIN revenue_agg rva

    ON rva.content_id = cm.content_id

   AND rva.month_start = cm.month_start


-- ===========================================================
-- RATINGS
-- ===========================================================

LEFT JOIN rating_agg rta

    ON rta.content_id = cm.content_id

   AND rta.month_start = cm.month_start


-- ===========================================================
-- WISHLIST
-- ===========================================================

LEFT JOIN wishlist_agg wa

    ON wa.content_id = cm.content_id

   AND wa.month_start = cm.month_start


-- ===========================================================
-- IDEMPOTENCY
-- ===========================================================

ON CONFLICT
(
    content_sk,
    date_key
)

DO UPDATE

SET
    total_streams =
        EXCLUDED.total_streams,

    total_rental_count =
        EXCLUDED.total_rental_count,

    revenue_generated =
        EXCLUDED.revenue_generated,

    average_customer_rating =
        EXCLUDED.average_customer_rating,

    wishlist_addition_count =
        EXCLUDED.wishlist_addition_count
        
RETURNING (xmax = 0) AS is_insert;