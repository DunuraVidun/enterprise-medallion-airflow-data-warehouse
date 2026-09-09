-- ===========================================================
-- GOLD FACT INVENTORY DAILY UTILIZATION
--
-- Grain:
-- One row per physical inventory item per day
--
-- Measures:
-- 1. Number of rentals
-- 2. Number of returns
-- 3. Number of days available
-- 4. Utilisation percentage
-- ===========================================================


WITH


-- ===========================================================
-- INVENTORY DATE SPINE
--
-- One row per inventory item per date from its purchase
-- date until today.
-- ===========================================================

inventory_dates AS
(
    SELECT
        ii.inventory_id,
        ii.barcode,

        d.date_key,
        d.full_date

    FROM silver.inventory_item ii

    JOIN gold.dim_date d

        ON d.full_date >= COALESCE(
            ii.purchase_date,
            DATE '2023-01-01'
        )

       AND d.full_date <= CURRENT_DATE

    WHERE ii.barcode IS NOT NULL
),


-- ===========================================================
-- RENTALS STARTED PER DAY
-- ===========================================================

rental_agg AS
(
    SELECT
        inventory_id,
        rental_date AS activity_date,

        COUNT(rental_id) AS rental_count

    FROM silver.rental

    WHERE rental_date IS NOT NULL

    GROUP BY
        inventory_id,
        rental_date
),


-- ===========================================================
-- RETURNS PER DAY
-- ===========================================================

return_agg AS
(
    SELECT
        inventory_id,
        return_date AS activity_date,

        COUNT(rental_id) AS return_count

    FROM silver.rental

    WHERE return_date IS NOT NULL

    GROUP BY
        inventory_id,
        return_date
),


-- ===========================================================
-- DETERMINE WHETHER ITEM WAS RENTED ON EACH DATE
--
-- An inventory item is considered utilised when:
--
-- rental_date <= current date
--
-- and
--
-- return_date is NULL
-- OR
-- return_date >= current date
-- ===========================================================

inventory_status AS
(
    SELECT
        id.inventory_id,
        id.barcode,
        id.date_key,
        id.full_date,

        CASE

            WHEN EXISTS
            (
                SELECT 1

                FROM silver.rental r

                WHERE r.inventory_id =
                      id.inventory_id

                  AND r.rental_date <=
                      id.full_date

                  AND
                  (
                      r.return_date IS NULL

                      OR

                      r.return_date >=
                      id.full_date
                  )
            )

            THEN 1

            ELSE 0

        END AS is_utilised

    FROM inventory_dates id
)


-- ===========================================================
-- LOAD FACT TABLE
-- ===========================================================

INSERT INTO gold.fact_inventory_daily_utilization
(
    inventory_sk,
    date_key,

    rental_count,
    return_count,

    days_available,

    utilisation_percentage
)

SELECT
    di.inventory_sk,

    ist.date_key,

    COALESCE(
        ra.rental_count,
        0
    ),

    COALESCE(
        rta.return_count,
        0
    ),


    -- =======================================================
    -- AVAILABLE = 1
    -- UTILIZED  = 0
    -- =======================================================

    CASE

        WHEN ist.is_utilised = 1
            THEN 0

        ELSE 1

    END AS days_available,


    -- =======================================================
    -- DAILY UTILISATION
    -- =======================================================

    CASE

        WHEN ist.is_utilised = 1
            THEN 100.00

        ELSE 0.00

    END::DECIMAL(5,2)
        AS utilisation_percentage

FROM inventory_status ist


-- ===========================================================
-- INVENTORY DIMENSION LOOKUP
-- ===========================================================

JOIN gold.dim_inventory_item di

    ON di.barcode = ist.barcode

   AND ist.full_date >= di.effective_from

   AND (
        ist.full_date < di.effective_to
        OR di.effective_to IS NULL
       )


-- ===========================================================
-- RENTALS
-- ===========================================================

LEFT JOIN rental_agg ra

    ON ra.inventory_id =
       ist.inventory_id

   AND ra.activity_date =
       ist.full_date


-- ===========================================================
-- RETURNS
-- ===========================================================

LEFT JOIN return_agg rta

    ON rta.inventory_id =
       ist.inventory_id

   AND rta.activity_date =
       ist.full_date


-- ===========================================================
-- IDEMPOTENCY
-- ===========================================================

ON CONFLICT
(
    inventory_sk,
    date_key
)

DO UPDATE

SET
    rental_count =
        EXCLUDED.rental_count,

    return_count =
        EXCLUDED.return_count,

    days_available =
        EXCLUDED.days_available,

    utilisation_percentage =
        EXCLUDED.utilisation_percentage

RETURNING (xmax = 0) AS is_insert;