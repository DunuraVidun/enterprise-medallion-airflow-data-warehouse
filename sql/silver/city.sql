WITH cleaned AS
(
    SELECT
        city_id,

        country_id,

        NULLIF(TRIM(city_name), '') AS city_name,

        created_at,
        updated_at,
        load_timestamp,
        source_system,

        ROW_NUMBER() OVER
        (
            PARTITION BY city_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST,
                load_timestamp DESC
        ) AS rn

    FROM bronze.city

    WHERE city_id IS NOT NULL
)

SELECT
    city_id,
    country_id,
    city_name,

    created_at,
    updated_at,
    load_timestamp,
    source_system

FROM cleaned c

WHERE rn = 1

  -- Mandatory fields
  AND country_id IS NOT NULL
  AND city_name IS NOT NULL
  AND TRIM(city_name) <> ''

  -- Referential integrity
  AND EXISTS
  (
      SELECT 1
      FROM silver.country co
      WHERE co.country_id = c.country_id
  );