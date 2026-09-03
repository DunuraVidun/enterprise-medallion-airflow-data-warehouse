WITH cleaned AS
(
    SELECT
        genre_id,

        INITCAP(NULLIF(TRIM(genre_name), '')) AS genre_name,

        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER
        (
            PARTITION BY genre_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST,
                load_timestamp DESC
        ) AS rn

    FROM bronze.genre

    WHERE genre_id IS NOT NULL
)

SELECT
    genre_id,
    genre_name,

    created_at,
    updated_at,
    load_timestamp,
    source_system

FROM cleaned

WHERE rn = 1

    -- Mandatory field validation
    AND genre_name IS NOT NULL

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