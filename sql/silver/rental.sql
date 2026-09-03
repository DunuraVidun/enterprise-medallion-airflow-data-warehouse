WITH cleaned AS
(
    SELECT
        rental_id,
        customer_id,
        inventory_id,

        rental_date,
        due_date,
        return_date,

        rental_fee,
        late_fee,

        UPPER(NULLIF(TRIM(status), '')) AS status,

        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER
        (
            PARTITION BY rental_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST,
                load_timestamp DESC
        ) AS rn

    FROM bronze.rental

    WHERE rental_id IS NOT NULL
)

SELECT
    rental_id,
    customer_id,
    inventory_id,
    rental_date,
    due_date,
    return_date,
    rental_fee,
    late_fee,
    status,
    created_at,
    updated_at,
    load_timestamp,
    source_system

FROM cleaned

WHERE rn = 1

    -- Mandatory fields
    AND customer_id IS NOT NULL
    AND inventory_id IS NOT NULL
    AND rental_date IS NOT NULL
    AND due_date IS NOT NULL
    AND rental_fee IS NOT NULL
    AND status IS NOT NULL

    -- Referential integrity: customer
    AND EXISTS
    (
        SELECT 1
        FROM silver.customer c
        WHERE c.customer_id = cleaned.customer_id
    )

    -- Referential integrity: inventory item
    AND EXISTS
    (
        SELECT 1
        FROM silver.inventory_item i
        WHERE i.inventory_id = cleaned.inventory_id
    )

    -- Valid rental dates
    AND rental_date <= CURRENT_DATE

    AND due_date >= rental_date

    AND (
        return_date IS NULL
        OR return_date >= rental_date
    )

    -- Monetary values cannot be negative
    AND rental_fee >= 0

    AND (
        late_fee IS NULL
        OR late_fee >= 0
    )

    -- Valid business status
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