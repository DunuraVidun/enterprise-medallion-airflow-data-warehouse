-- ===========================================================
-- GOLD DIM CUSTOMER
-- SCD TYPE 2
-- ===========================================================


-- ===========================================================
-- STEP 1
-- Close existing current records when customer attributes
-- have changed.
-- ===========================================================

UPDATE gold.dim_customer AS target

SET
    effective_to = source.updated_at,
    is_current = FALSE,
    updated_at = source.updated_at

FROM silver.customer_profile AS source

WHERE target.customer_no = source.customer_no

  AND target.is_current = TRUE

  AND source.updated_at IS NOT NULL

  AND source.updated_at > target.effective_from

  AND
    (
        target.full_name IS DISTINCT FROM source.full_name
    OR target.email IS DISTINCT FROM source.email
    OR target.age IS DISTINCT FROM source.age
    OR target.city_name IS DISTINCT FROM source.city_name
    OR target.country_name IS DISTINCT FROM source.country_name
    OR target.status IS DISTINCT FROM source.status
    );


-- ===========================================================
-- STEP 2
-- INSERT COMPLETELY NEW CUSTOMERS
--
-- A customer is considered new only when NO historical
-- record exists for the business key.
-- ===========================================================

INSERT INTO gold.dim_customer
(
    customer_no,

    full_name,
    email,
    age,
    city_name,
    country_name,
    status,

    effective_from,
    effective_to,
    is_current,

    created_at,
    updated_at,
    source_system
)

SELECT
    source.customer_no,

    source.full_name,
    source.email,
    source.age,
    source.city_name,
    source.country_name,
    source.status,

    COALESCE(
        source.created_at,
        source.updated_at
    ) AS effective_from,

    TIMESTAMP '9999-12-31 23:59:59'
        AS effective_to,

    TRUE AS is_current,

    source.created_at,
    source.updated_at,
    source.source_system

FROM silver.customer_profile AS source

WHERE NOT EXISTS
(
    SELECT 1

    FROM gold.dim_customer existing

    WHERE existing.customer_no =
          source.customer_no
)

AND COALESCE(
        source.created_at,
        source.updated_at
    ) IS NOT NULL;


-- ===========================================================
-- STEP 3
-- INSERT NEW SCD TYPE 2 VERSIONS
--
-- The previous version has already been closed by STEP 1.
--
-- The existence of a historical record confirms that this is
-- an existing customer receiving a new SCD2 version.
-- ===========================================================

INSERT INTO gold.dim_customer
(
    customer_no,

    full_name,
    email,
    age,
    city_name,
    country_name,
    status,

    effective_from,
    effective_to,
    is_current,

    created_at,
    updated_at,
    source_system
)

SELECT
    source.customer_no,

    source.full_name,
    source.email,
    source.age,
    source.city_name,
    source.country_name,
    source.status,

    source.updated_at AS effective_from,

    TIMESTAMP '9999-12-31 23:59:59'
        AS effective_to,

    TRUE AS is_current,

    source.created_at,
    source.updated_at,
    source.source_system

FROM silver.customer_profile AS source

WHERE source.updated_at IS NOT NULL

  -- Existing customer must already have historical data
  AND EXISTS
  (
      SELECT 1

      FROM gold.dim_customer historical

      WHERE historical.customer_no =
            source.customer_no
  )

  -- There must not already be a current version.
  -- STEP 1 closes the old version when a real change occurs.
  AND NOT EXISTS
  (
      SELECT 1

      FROM gold.dim_customer current_version

      WHERE current_version.customer_no =
            source.customer_no

        AND current_version.is_current = TRUE
  )

  -- Make sure the source timestamp actually represents
  -- the end of the previous SCD2 version.
  AND EXISTS
  (
      SELECT 1

      FROM gold.dim_customer previous_version

      WHERE previous_version.customer_no =
            source.customer_no

        AND previous_version.is_current = FALSE

        AND previous_version.effective_to =
            source.updated_at
  );

