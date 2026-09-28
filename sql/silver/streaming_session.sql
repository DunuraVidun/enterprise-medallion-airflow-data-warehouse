WITH cleaned AS
(
    SELECT
        stream_id,

        customer_id,
        content_id,
        device_id,

        start_time,
        end_time,

        watch_duration,
        completion_percentage,

        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER
        (
            PARTITION BY stream_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST,
                load_timestamp DESC
        ) AS rn

    FROM bronze.streaming_session

    WHERE stream_id IS NOT NULL
)

SELECT
    stream_id,
    customer_id,
    content_id,
    device_id,

    start_time,
    end_time,

    watch_duration,
    completion_percentage,

    created_at,
    updated_at,
    load_timestamp,
    source_system

FROM cleaned

WHERE rn = 1

    -- Mandatory fields
    AND customer_id IS NOT NULL
    AND content_id IS NOT NULL
    AND start_time IS NOT NULL

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

    -- Valid session dates
    AND (
        end_time IS NULL
        OR end_time >= start_time
    )

    -- Watch duration cannot be negative
    AND (
        watch_duration IS NULL
        OR watch_duration >= 0
    )

    -- Completion percentage must be between 0 and 100
    AND (
        completion_percentage IS NULL
        OR completion_percentage BETWEEN 0 AND 100
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