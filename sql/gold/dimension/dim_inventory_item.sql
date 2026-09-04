-- ===========================================================
-- GOLD DIM INVENTORY ITEM
-- SCD TYPE 2
-- ===========================================================


-- ===========================================================
-- STEP 1
-- Close existing current records when inventory item
-- attributes have changed.
-- ===========================================================

UPDATE gold.dim_inventory_item AS target

SET
    effective_to = CURRENT_TIMESTAMP,
    is_current = FALSE,
    updated_at = CURRENT_TIMESTAMP

FROM silver.inventory_item_profile AS source

WHERE target.barcode = source.barcode

  AND target.is_current = TRUE

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
-- Insert:
--
-- 1. Completely new inventory items
-- 2. New versions of inventory items whose previous version
--    was closed in STEP 1
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

    CURRENT_TIMESTAMP AS effective_from,

    TIMESTAMP '9999-12-31 23:59:59'
        AS effective_to,

    TRUE AS is_current,

    source.created_at,
    source.updated_at,
    source.source_system

FROM silver.inventory_item_profile AS source

LEFT JOIN gold.dim_inventory_item AS target

    ON target.barcode = source.barcode

    AND target.is_current = TRUE

WHERE target.inventory_sk IS NULL;