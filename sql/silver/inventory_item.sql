WITH cleaned AS
(
    SELECT
        inventory_id,
        content_id,
        warehouse_id,

        NULLIF(TRIM(barcode), '') AS barcode,

        purchase_date,

        UPPER(NULLIF(TRIM(item_condition), '')) AS item_condition,

        UPPER(NULLIF(TRIM(status), '')) AS status,

        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER
        (
            PARTITION BY inventory_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST,
                load_timestamp DESC
        ) AS rn

    FROM bronze.inventory_item

    WHERE inventory_id IS NOT NULL
)

SELECT
    inventory_id,
    content_id,
    warehouse_id,
    barcode,
    purchase_date,
    item_condition,
    status,
    created_at,
    updated_at,
    load_timestamp,
    source_system

FROM cleaned

WHERE rn = 1

    -- Mandatory fields
    AND content_id IS NOT NULL
    AND warehouse_id IS NOT NULL
    AND barcode IS NOT NULL

    -- Barcode must be unique
    AND NOT EXISTS
    (
        SELECT 1
        FROM cleaned i
        WHERE i.barcode = cleaned.barcode
          AND i.rn = 1
          AND i.inventory_id <> cleaned.inventory_id
    )

    -- Content must exist
    AND EXISTS
    (
        SELECT 1
        FROM silver.content c
        WHERE c.content_id = cleaned.content_id
    )

    -- Warehouse must exist
    AND EXISTS
    (
        SELECT 1
        FROM silver.warehouse w
        WHERE w.warehouse_id = cleaned.warehouse_id
    )

    -- Purchase date cannot be in the future
    AND (
        purchase_date IS NULL
        OR purchase_date <= CURRENT_DATE
    )

    -- Item condition must be present
    AND item_condition IS NOT NULL

    -- Status must be present
    AND status IS NOT NULL

    -- Valid audit timestamps
    AND (
        created_at IS NULL
        OR created_at <= CURRENT_TIMESTAMP
    )

    AND (
        updated_at IS NULL
        OR updated_at <= CURRENT_TIMESTAMP
    )

    -- Updated timestamp cannot be before created timestamp
    AND (
        created_at IS NULL
        OR updated_at IS NULL
        OR updated_at >= created_at
    );