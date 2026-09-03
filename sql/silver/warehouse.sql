WITH cleaned AS
(
    SELECT
        warehouse_id,
        city_id,

        INITCAP(NULLIF(TRIM(warehouse_name), '')) AS warehouse_name,

        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER
        (
            PARTITION BY warehouse_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST,
                load_timestamp DESC
        ) AS rn

    FROM bronze.warehouse

    WHERE warehouse_id IS NOT NULL
)

SELECT
    warehouse_id,
    city_id,
    warehouse_name,
    created_at,
    updated_at,
    load_timestamp,
    source_system

FROM cleaned

WHERE rn = 1

    -- Referential integrity: city must exist
    AND (
        city_id IS NULL
        OR EXISTS
        (
            SELECT 1
            FROM silver.city c
            WHERE c.city_id = cleaned.city_id
        )
    )

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