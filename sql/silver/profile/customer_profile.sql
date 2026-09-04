SELECT
    c.customer_id,

    c.customer_no,

    CONCAT_WS(
        ' ',
        NULLIF(TRIM(c.first_name), ''),
        NULLIF(TRIM(c.last_name), '')
    ) AS full_name,

    EXTRACT(
        YEAR FROM AGE(CURRENT_DATE, c.date_of_birth)
    )::INT AS age,

    ci.city_name,

    co.country_name,

    c.created_at,

    c.updated_at,

    CURRENT_TIMESTAMP AS load_timestamp,

    c.source_system

FROM silver.customer AS c

LEFT JOIN silver.customer_address AS ca
    ON c.customer_id = ca.customer_id
    AND ca.address_type = 'HOME'

LEFT JOIN silver.city AS ci
    ON ca.city_id = ci.city_id

LEFT JOIN silver.country AS co
    ON ci.country_id = co.country_id;