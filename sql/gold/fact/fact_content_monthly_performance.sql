-- ===========================================================
-- GOLD FACT CONTENT MONTHLY PERFORMANCE
--
-- Grain:
-- One row per content item per month
--
-- SCD2 RULE:
-- The content dimension version valid at the END of the
-- month is used for the monthly fact row.
--
-- FACT TABLE IS FULLY REBUILT ON EACH RUN.
-- ===========================================================


-- ===========================================================
-- STEP 0
-- FULL REFRESH
-- ===========================================================

TRUNCATE TABLE gold.fact_content_monthly_performance;


WITH


-- ===========================================================
-- CONTENT MONTH SPINE
--
-- One row for every content item for every month from
-- release month through the current month.
--
-- date_key = first day of month.
-- ===========================================================

content_months AS
(
    SELECT
        c.content_id,

        d.date_key,

        d.full_date AS month_start

    FROM silver.content AS c

    JOIN gold.dim_date AS d

        ON d.day = 1

       AND d.full_date <=
           DATE_TRUNC(
               'month',
               CURRENT_DATE
           )::DATE

       AND d.full_date >=
           GREATEST
           (
               COALESCE
               (
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

    FROM silver.rental AS r

    JOIN silver.inventory_item AS ii

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
-- payment -> rental -> inventory_item -> content
--
-- Only completed payments are included.
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

    FROM silver.payment AS p

    JOIN silver.rental AS r

        ON r.rental_id = p.rental_id

    JOIN silver.inventory_item AS ii

        ON ii.inventory_id = r.inventory_id

    WHERE p.payment_date IS NOT NULL

      AND p.rental_id IS NOT NULL

      AND p.status = 'COMPLETED'

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

        COUNT(wishlist_id) AS wishlist_addition_count

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
    ) AS total_streams,

    COALESCE(
        ra.total_rental_count,
        0
    ) AS total_rental_count,

    COALESCE(
        rva.revenue_generated,
        0
    ) AS revenue_generated,

    COALESCE(
        rta.average_customer_rating,
        0
    ) AS average_customer_rating,

    COALESCE(
        wa.wishlist_addition_count,
        0
    ) AS wishlist_addition_count

FROM content_months AS cm


-- ===========================================================
-- CONTENT DIMENSION SCD2 LOOKUP
--
-- BUSINESS RULE:
-- Use the content dimension version that was valid at the
-- END of the month.
--
-- END OF MONTH =
-- month_start + 1 month - 1 microsecond
--
-- Handles both:
--   1. Closed SCD2 records
--   2. Current/open-ended SCD2 record (effective_to IS NULL)
-- ===========================================================

JOIN gold.dim_content AS dc

    ON dc.content_id = cm.content_id

   AND
       (
           cm.month_start
           + INTERVAL '1 month'
           - INTERVAL '1 microsecond'
       ) >= dc.effective_from

   AND
       (
           dc.effective_to IS NULL

           OR

           (
               cm.month_start
               + INTERVAL '1 month'
               - INTERVAL '1 microsecond'
           ) < dc.effective_to
       )


-- ===========================================================
-- STREAMING
-- ===========================================================

LEFT JOIN stream_agg AS sa

    ON sa.content_id = cm.content_id

   AND sa.month_start = cm.month_start


-- ===========================================================
-- RENTALS
-- ===========================================================

LEFT JOIN rental_agg AS ra

    ON ra.content_id = cm.content_id

   AND ra.month_start = cm.month_start


-- ===========================================================
-- REVENUE
-- ===========================================================

LEFT JOIN revenue_agg AS rva

    ON rva.content_id = cm.content_id

   AND rva.month_start = cm.month_start


-- ===========================================================
-- RATINGS
-- ===========================================================

LEFT JOIN rating_agg AS rta

    ON rta.content_id = cm.content_id

   AND rta.month_start = cm.month_start


-- ===========================================================
-- WISHLIST
-- ===========================================================

LEFT JOIN wishlist_agg AS wa

    ON wa.content_id = cm.content_id

   AND wa.month_start = cm.month_start;


-- ===========================================================
-- END
-- ===========================================================