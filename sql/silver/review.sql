WITH cleaned AS
(
    SELECT
        review_id,

        customer_id,
        content_id,

        rating,

        NULLIF(TRIM(review_text), '') AS review_text,

        review_date,

        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER
        (
            PARTITION BY review_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST,
                load_timestamp DESC
        ) AS rn

    FROM bronze.review

    WHERE review_id IS NOT NULL
)

SELECT
    review_id,

    customer_id,
    content_id,

    rating,
    review_text,
    review_date,

    created_at,
    updated_at,
    load_timestamp,
    source_system

FROM cleaned

WHERE rn = 1

    -- Mandatory fields
    AND customer_id IS NOT NULL
    AND content_id IS NOT NULL
    AND rating IS NOT NULL
    AND review_date IS NOT NULL

    -- Referential integrity: customer must exist
    AND EXISTS
    (
        SELECT 1
        FROM silver.customer c
        WHERE c.customer_id = cleaned.customer_id
    )

    -- Referential integrity: content must exist
    AND EXISTS
    (
        SELECT 1
        FROM silver.content c
        WHERE c.content_id = cleaned.content_id
    )

    -- Rating must be between 0 and 5
    AND rating BETWEEN 0 AND 5

    -- Review date cannot be in the future
    AND review_date <= CURRENT_TIMESTAMP

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