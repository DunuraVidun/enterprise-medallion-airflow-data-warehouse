WITH cleaned AS
(
    SELECT
        ticket_id,
        customer_id,
        category_id,

        opened_date,
        closed_date,

        UPPER(NULLIF(TRIM(priority), '')) AS priority,

        UPPER(NULLIF(TRIM(status), '')) AS status,

        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER
        (
            PARTITION BY ticket_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST,
                load_timestamp DESC
        ) AS rn

    FROM bronze.support_ticket

    WHERE ticket_id IS NOT NULL
)

SELECT
    ticket_id,
    customer_id,
    category_id,
    opened_date,
    closed_date,
    priority,
    status,
    created_at,
    updated_at,
    load_timestamp,
    source_system

FROM cleaned

WHERE rn = 1

    -- =========================================================
    -- 1. MANDATORY FIELD VALIDATION
    -- =========================================================

    AND customer_id IS NOT NULL
    AND opened_date IS NOT NULL
    AND priority IS NOT NULL
    AND status IS NOT NULL


    -- =========================================================
    -- 2. REFERENTIAL INTEGRITY
    -- =========================================================

    AND EXISTS
    (
        SELECT 1
        FROM silver.customer c
        WHERE c.customer_id = cleaned.customer_id
    )


    -- =========================================================
    -- 3. VALID DATE VALIDATION
    -- =========================================================

    -- Ticket cannot be opened in the future
    AND opened_date <= CURRENT_TIMESTAMP

    -- Closed date cannot be before opened date
    AND (
        closed_date IS NULL
        OR closed_date >= opened_date
    )

    -- Closed date cannot be in the future
    AND (
        closed_date IS NULL
        OR closed_date <= CURRENT_TIMESTAMP
    )


    -- =========================================================
    -- 4. VALID BUSINESS STATUS
    -- =========================================================

    AND status IS NOT NULL


    -- =========================================================
    -- 5. VALID BUSINESS PRIORITY
    -- =========================================================

    AND priority IS NOT NULL


    -- =========================================================
    -- 6. AUDIT TIMESTAMP VALIDATION
    -- =========================================================

    AND (
        created_at IS NULL
        OR created_at <= CURRENT_TIMESTAMP
    )

    AND (
        updated_at IS NULL
        OR updated_at <= CURRENT_TIMESTAMP
    )

    AND (
        created_at IS NULL
        OR updated_at IS NULL
        OR updated_at >= created_at
    );