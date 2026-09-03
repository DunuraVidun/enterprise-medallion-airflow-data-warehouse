WITH cleaned AS
(
    SELECT
        customer_id,

        NULLIF(TRIM(customer_no), '') AS customer_no,

        NULLIF(TRIM(first_name), '') AS first_name,
        NULLIF(TRIM(last_name), '') AS last_name,

        LOWER(NULLIF(TRIM(email), '')) AS email,

        NULLIF(TRIM(phone), '') AS phone,

        date_of_birth,

        UPPER(NULLIF(TRIM(gender), '')) AS gender,

        registration_date,

        UPPER(NULLIF(TRIM(status), ''))AS status,

        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER
        (
            PARTITION BY customer_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST,
                load_timestamp DESC
        ) AS rn

    FROM bronze.customer

    WHERE customer_id IS NOT NULL
)

SELECT
    customer_id,
    customer_no,

    first_name,
    last_name,

    email,
    phone,

    date_of_birth,
    gender,
    registration_date,
    status,

    created_at,
    updated_at,
    load_timestamp,
    source_system

FROM cleaned
WHERE rn = 1

    -- Mandatory fields
    AND customer_no IS NOT NULL
    AND first_name IS NOT NULL
    AND last_name IS NOT NULL

    -- Valid dates
    AND COALESCE(date_of_birth <= CURRENT_DATE, TRUE)
    AND COALESCE(registration_date <= CURRENT_DATE, TRUE)

    -- Valid date relationship
    AND COALESCE(date_of_birth <= registration_date, TRUE);