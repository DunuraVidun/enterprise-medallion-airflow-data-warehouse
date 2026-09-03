WITH cleaned AS
(
    SELECT
        content_type_id,

        NULLIF(TRIM(content_type), '') AS content_type,

        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER
        (
            PARTITION BY content_type_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST,
                load_timestamp DESC
        ) AS rn

    FROM bronze.content_type

    WHERE content_type_id IS NOT NULL
)

SELECT
    content_type_id,
    content_type,

    created_at,
    updated_at,
    load_timestamp,
    source_system

FROM cleaned

WHERE rn = 1

    -- Mandatory field validation
    AND content_type IS NOT NULL

    -- Valid dates
    AND (
        created_at IS NULL
        OR created_at <= CURRENT_TIMESTAMP
    )

    AND (
        updated_at IS NULL
        OR updated_at <= CURRENT_TIMESTAMP
    )

    -- Date relationship
    AND (
        created_at IS NULL
        OR updated_at IS NULL
        OR updated_at >= created_at
    );