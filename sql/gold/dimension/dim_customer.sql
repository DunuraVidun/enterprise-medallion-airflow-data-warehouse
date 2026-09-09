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
    effective_to = CURRENT_TIMESTAMP,
    is_current = FALSE,
    updated_at = CURRENT_TIMESTAMP

FROM silver.customer_profile AS source

WHERE target.customer_no = source.customer_no

  AND target.is_current = TRUE

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
-- Insert:
--
-- 1. Completely new customers
-- 2. New versions of customers whose previous version
--    was closed in STEP 1
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

    CURRENT_TIMESTAMP AS effective_from,

    TIMESTAMP '9999-12-31 23:59:59'
        AS effective_to,

    TRUE AS is_current,

    source.created_at,
    source.updated_at,
    source.source_system

FROM silver.customer_profile AS source

LEFT JOIN gold.dim_customer AS target

    ON target.customer_no = source.customer_no

    AND target.is_current = TRUE

WHERE target.customer_sk IS NULL;