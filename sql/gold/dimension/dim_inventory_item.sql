-- ===========================================================
-- GOLD DIM INVENTORY ITEM
-- SCD TYPE 2
-- ===========================================================


-- ===========================================================
-- STEP 1
-- Close existing current records when inventory item
-- attributes have changed.
--
-- The source updated_at timestamp is used as the SCD2
-- effective boundary.
-- ===========================================================

UPDATE gold.dim_inventory_item AS target

SET
    effective_to = source.updated_at,
    is_current = FALSE,
    updated_at = source.updated_at

FROM silver.inventory_item_profile AS source

WHERE target.barcode = source.barcode

  AND target.is_current = TRUE

  AND source.updated_at IS NOT NULL

  AND source.updated_at > target.effective_from

  AND
  (
       target.content_title
       IS DISTINCT FROM source.content_title

    OR target.warehouse_name
       IS DISTINCT FROM source.warehouse_name

    OR target.warehouse_city
       IS DISTINCT FROM source.warehouse_city

    OR target.item_condition
       IS DISTINCT FROM source.item_condition
  );


-- ===========================================================
-- STEP 2
-- INSERT COMPLETELY NEW INVENTORY ITEMS
--
-- An inventory item is considered new only when NO historical
-- record exists for the business key (barcode).
-- ===========================================================

INSERT INTO gold.dim_inventory_item
(
    barcode,

    content_title,
    warehouse_name,
    warehouse_city,
    item_condition,

    effective_from,
    effective_to,
    is_current,

    created_at,
    updated_at,
    source_system
)

SELECT
    source.barcode,

    source.content_title,
    source.warehouse_name,
    source.warehouse_city,
    source.item_condition,

    DATE '2018-01-01' AS effective_from,

    TIMESTAMP '9999-12-31 23:59:59'
        AS effective_to,

    TRUE AS is_current,

    source.created_at,
    source.updated_at,
    source.source_system

FROM silver.inventory_item_profile AS source

WHERE NOT EXISTS
(
    SELECT 1

    FROM gold.dim_inventory_item AS existing

    WHERE existing.barcode = source.barcode
);


-- ===========================================================
-- STEP 3
-- INSERT NEW SCD TYPE 2 VERSIONS
--
-- The previous version has already been closed by STEP 1.
--
-- The existence of a historical record confirms that this
-- inventory item already existed.
-- ===========================================================

INSERT INTO gold.dim_inventory_item
(
    barcode,

    content_title,
    warehouse_name,
    warehouse_city,
    item_condition,

    effective_from,
    effective_to,
    is_current,

    created_at,
    updated_at,
    source_system
)

SELECT
    source.barcode,

    source.content_title,
    source.warehouse_name,
    source.warehouse_city,
    source.item_condition,

    source.updated_at AS effective_from,

    TIMESTAMP '9999-12-31 23:59:59'
        AS effective_to,

    TRUE AS is_current,

    source.created_at,
    source.updated_at,
    source.source_system

FROM silver.inventory_item_profile AS source

WHERE source.updated_at IS NOT NULL


  -- ---------------------------------------------------------
  -- Inventory item must already exist historically.
  -- ---------------------------------------------------------

  AND EXISTS
  (
      SELECT 1

      FROM gold.dim_inventory_item AS historical

      WHERE historical.barcode = source.barcode
  )


  -- ---------------------------------------------------------
  -- There must not already be a current version.
  --
  -- STEP 1 closes the previous version when a real change
  -- occurs.
  -- ---------------------------------------------------------

  AND NOT EXISTS
  (
      SELECT 1

      FROM gold.dim_inventory_item AS current_version

      WHERE current_version.barcode = source.barcode

        AND current_version.is_current = TRUE
  )


  -- ---------------------------------------------------------
  -- Confirm that STEP 1 actually closed the previous version
  -- at this exact source change timestamp.
  -- ---------------------------------------------------------

  AND EXISTS
  (
      SELECT 1

      FROM gold.dim_inventory_item AS previous_version

      WHERE previous_version.barcode = source.barcode

        AND previous_version.is_current = FALSE

        AND previous_version.effective_to =
            source.updated_at
  );