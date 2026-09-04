-- ===========================================================
-- SILVER INVENTORY ITEM PROFILE
-- ===========================================================

SELECT
    ii.inventory_id,
    ii.barcode,

    c.title AS content_title,

    w.warehouse_name,

    ci.city_name AS warehouse_city,

    ii.item_condition,

    ii.created_at,
    ii.updated_at,
    CURRENT_TIMESTAMP AS load_timestamp,
    ii.source_system

FROM silver.inventory_item AS ii

LEFT JOIN silver.content AS c
    ON ii.content_id = c.content_id

LEFT JOIN silver.warehouse AS w
    ON ii.warehouse_id = w.warehouse_id

LEFT JOIN silver.city AS ci
    ON w.city_id = ci.city_id;