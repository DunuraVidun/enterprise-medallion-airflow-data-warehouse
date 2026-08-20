SELECT
    *,
    CURRENT_TIMESTAMP AS load_timestamp,
    'ABC_HUB_OLTP' AS source_system
FROM inventory_mgmt.warehouse;