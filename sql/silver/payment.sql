WITH cleaned AS
(
    SELECT
        payment_id,
        customer_id,
        rental_id,
        subscription_id,
        payment_method_id,

        amount,
        payment_date,

        UPPER(NULLIF(TRIM(payment_type), '')) AS payment_type,

        UPPER(NULLIF(TRIM(status), '')) AS status,

        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER
        (
            PARTITION BY payment_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST,
                load_timestamp DESC
        ) AS rn

    FROM bronze.payment

    WHERE payment_id IS NOT NULL
)

SELECT
    payment_id,
    customer_id,
    rental_id,
    subscription_id,
    payment_method_id,
    amount,
    payment_date,
    payment_type,
    status,
    created_at,
    updated_at,
    load_timestamp,
    source_system

FROM cleaned

WHERE rn = 1

    -- Mandatory fields
    AND customer_id IS NOT NULL
    AND amount IS NOT NULL
    AND payment_date IS NOT NULL
    AND payment_type IS NOT NULL
    AND status IS NOT NULL

    -- Referential integrity: customer
    AND EXISTS
    (
        SELECT 1
        FROM silver.customer c
        WHERE c.customer_id = cleaned.customer_id
    )

    AND
    (
        rental_id IS NULL
        OR EXISTS
        (
            SELECT 1
            FROM silver.rental r
            WHERE r.rental_id = cleaned.rental_id
        )
    )

    -- Valid payment date
    AND payment_date <= CURRENT_TIMESTAMP

    -- Valid monetary value
    AND amount >= 0

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