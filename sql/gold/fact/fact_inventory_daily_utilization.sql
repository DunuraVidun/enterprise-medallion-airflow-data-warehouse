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
--
-- SCD2 RULE:
-- The inventory dimension version valid at the END of the
-- activity day is used.
--
-- FACT TABLE IS FULLY REBUILT ON EACH RUN.
-- ===========================================================


-- ===========================================================
-- STEP 0
-- FULL REFRESH
-- ===========================================================

TRUNCATE TABLE gold.fact_inventory_daily_utilization;


WITH


-- ===========================================================
-- STEP 1
-- INVENTORY DATE SPINE
--
-- One row per inventory item per date from purchase date
-- through the current date.
--
-- If purchase_date is NULL, use 2023-01-01 as the
-- analytical fallback date.
-- ===========================================================

inventory_dates AS
(
    SELECT
        ii.inventory_id,
        ii.barcode,

        d.date_key,
        d.full_date

    FROM silver.inventory_item AS ii

    JOIN gold.dim_date AS d

        ON d.full_date >= COALESCE(
            ii.purchase_date,
            DATE '2023-01-01'
        )

       AND d.full_date <= CURRENT_DATE

    WHERE ii.barcode IS NOT NULL
),


-- ===========================================================
-- STEP 2
-- RENTALS STARTED PER DAY
--
-- Counts how many rental transactions started for each
-- inventory item on each date.
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
-- STEP 3
-- RETURNS PER DAY
--
-- Counts how many rental items were returned for each
-- inventory item on each date.
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
-- STEP 4
-- DETERMINE DAILY UTILISATION
--
-- An inventory item is considered UTILIZED when:
--
--     rental_date <= activity_date
--
-- AND
--
--     return_date IS NULL
--     OR
--     return_date > activity_date
--
-- Therefore, the return date itself is considered AVAILABLE.
--
-- EXISTS is used so multiple rental records do not duplicate
-- inventory-date rows.
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

                FROM silver.rental AS r

                WHERE r.inventory_id =
                      id.inventory_id

                  AND r.rental_date <=
                      id.full_date

                  AND
                  (
                      r.return_date IS NULL

                      OR

                      r.return_date >
                      id.full_date
                  )
            )

            THEN 1

            ELSE 0

        END AS is_utilised

    FROM inventory_dates AS id
)


-- ===========================================================
-- STEP 5
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


    -- =======================================================
    -- RENTAL COUNT
    -- =======================================================

    COALESCE(
        ra.rental_count,
        0
    ) AS rental_count,


    -- =======================================================
    -- RETURN COUNT
    -- =======================================================

    COALESCE(
        rta.return_count,
        0
    ) AS return_count,


    -- =======================================================
    -- DAYS AVAILABLE
    --
    -- Utilized = 0 available days
    -- Available = 1 available day
    -- =======================================================

    CASE

        WHEN ist.is_utilised = 1
            THEN 0

        ELSE 1

    END AS days_available,


    -- =======================================================
    -- DAILY UTILISATION
    --
    -- Utilized = 100%
    -- Available = 0%
    -- =======================================================

    CASE

        WHEN ist.is_utilised = 1
            THEN 100.00

        ELSE 0.00

    END::DECIMAL(5,2)
        AS utilisation_percentage


FROM inventory_status AS ist


-- ===========================================================
-- STEP 6
-- INVENTORY DIMENSION SCD2 LOOKUP
--
-- The dimension version valid at the END OF THE DAY
-- is selected.
--
-- Example:
--
-- Activity date = 2025-06-06
-- End of day    = 2025-06-06 23:59:59.999999
--
-- That timestamp must fall within the SCD2 validity period.
-- ===========================================================

JOIN gold.dim_inventory_item AS di

    ON di.barcode =
       ist.barcode

   AND
       (
           ist.full_date
           + INTERVAL '1 day'
           - INTERVAL '1 microsecond'
       ) >= di.effective_from

   AND
       (
           di.effective_to IS NULL

           OR

           (
               ist.full_date
               + INTERVAL '1 day'
               - INTERVAL '1 microsecond'
           ) < di.effective_to
       )


-- ===========================================================
-- STEP 7
-- RENTAL COUNTS
-- ===========================================================

LEFT JOIN rental_agg AS ra

    ON ra.inventory_id =
       ist.inventory_id

   AND ra.activity_date =
       ist.full_date


-- ===========================================================
-- STEP 8
-- RETURN COUNTS
-- ===========================================================

LEFT JOIN return_agg AS rta

    ON rta.inventory_id =
       ist.inventory_id

   AND rta.activity_date =
       ist.full_date;


-- ===========================================================
-- END
-- ===========================================================