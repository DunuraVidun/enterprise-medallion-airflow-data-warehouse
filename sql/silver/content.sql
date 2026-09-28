WITH cleaned AS
(
    SELECT
        content_id,

        content_type_id,

        NULLIF(TRIM(title), '') AS title,

        release_date,

        duration_minutes,

        INITCAP(NULLIF(TRIM(language), '')) AS language,

        UPPER(NULLIF(TRIM(age_rating), '')) AS age_rating,

        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER
        (
            PARTITION BY content_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST,
                load_timestamp DESC
        ) AS rn

    FROM bronze.content

    WHERE content_id IS NOT NULL
)

SELECT
    content_id,
    content_type_id,

    title,
    release_date,
    duration_minutes,
    language,
    age_rating,

    created_at,
    updated_at,
    load_timestamp,
    source_system

FROM cleaned

WHERE rn = 1

    -- Mandatory fields
    AND title IS NOT NULL
    AND content_type_id IS NOT NULL
    AND release_date IS NOT NULL

    -- Referential integrity
    AND EXISTS
    (
        SELECT 1
        FROM silver.content_type ct
        WHERE ct.content_type_id = cleaned.content_type_id
    )

    -- Valid release date
    AND release_date <= CURRENT_DATE

    -- Valid duration
    AND (
        duration_minutes IS NULL
        OR duration_minutes >= 0
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

    -- Updated timestamp cannot be before created timestamp
    AND (
        created_at IS NULL
        OR updated_at IS NULL
        OR updated_at >= created_at
    );