WITH cleaned AS
(
    SELECT
        address_id,
        customer_id,
        city_id,

        NULLIF(TRIM(address_line), '') AS address_line,

        NULLIF(TRIM(postal_code), '') AS postal_code,

        UPPER(NULLIF(TRIM(address_type), '')) AS address_type,

        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER
        (
            PARTITION BY address_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST,
                load_timestamp DESC
        ) AS rn

    FROM bronze.customer_address

    WHERE address_id IS NOT NULL
)

SELECT
    address_id,
    customer_id,
    city_id,

    address_line,
    postal_code,
    address_type,

    created_at,
    updated_at,
    load_timestamp,
    source_system

FROM cleaned

WHERE rn = 1

    -- Mandatory fields
    AND customer_id IS NOT NULL
    AND address_line IS NOT NULL

    -- Referential integrity: customer must exist
    AND EXISTS
    (
        SELECT 1
        FROM silver.customer c
        WHERE c.customer_id = cleaned.customer_id
    )

    -- Referential integrity: city is optional,
    -- but when provided it must exist
    AND
    (
        city_id IS NULL
        OR EXISTS
        (
            SELECT 1
            FROM silver.city c
            WHERE c.city_id = cleaned.city_id
        )
    )

    -- Valid dates
    AND (
        created_at IS NULL
        OR created_at <= CURRENT_TIMESTAMP
    )

    AND (
        updated_at IS NULL
        OR updated_at <= CURRENT_TIMESTAMP
    )

    -- Updated date should not be before created date
    AND (
        created_at IS NULL
        OR updated_at IS NULL
        OR updated_at >= created_at
    );