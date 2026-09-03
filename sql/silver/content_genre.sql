WITH cleaned AS
(
    SELECT
        content_id,
        genre_id,

        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER
        (
            PARTITION BY
                content_id,
                genre_id

            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST,
                load_timestamp DESC
        ) AS rn

    FROM bronze.content_genre

    WHERE content_id IS NOT NULL
      AND genre_id IS NOT NULL
)

SELECT
    content_id,
    genre_id,

    created_at,
    updated_at,
    load_timestamp,
    source_system

FROM cleaned

WHERE rn = 1

    -- Mandatory fields
    AND content_id IS NOT NULL
    AND genre_id IS NOT NULL

    -- Referential integrity: content must exist
    AND EXISTS
    (
        SELECT 1
        FROM silver.content c
        WHERE c.content_id = cleaned.content_id
    )

    -- Referential integrity: genre must exist
    AND EXISTS
    (
        SELECT 1
        FROM silver.genre g
        WHERE g.genre_id = cleaned.genre_id
    )

    -- Valid audit dates
    AND (
        created_at IS NULL
        OR created_at <= CURRENT_TIMESTAMP
    )

    AND (
        updated_at IS NULL
        OR updated_at <= CURRENT_TIMESTAMP
    )

    -- Updated date cannot be before created date
    AND (
        created_at IS NULL
        OR updated_at IS NULL
        OR updated_at >= created_at
    );