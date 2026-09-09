-- ===========================================================
-- GOLD DIM CONTENT
-- SCD TYPE 2
-- ===========================================================


-- ===========================================================
-- STEP 1
-- Close existing current records when content attributes
-- have changed.
--
-- The source updated_at timestamp is used as the SCD2
-- effective boundary.
-- ===========================================================

UPDATE gold.dim_content AS target

SET
    effective_to = source.updated_at,
    is_current = FALSE,
    updated_at = source.updated_at

FROM silver.content_profile AS source

WHERE target.content_id = source.content_id

  AND target.is_current = TRUE

  AND source.updated_at IS NOT NULL

  AND source.updated_at > target.effective_from

  AND
  (
       target.title IS DISTINCT FROM source.title

    OR target.content_type_name
       IS DISTINCT FROM source.content_type_name

    OR target.primary_genre
       IS DISTINCT FROM source.primary_genre

    OR target.release_year
       IS DISTINCT FROM source.release_year
  );


-- ===========================================================
-- STEP 2
-- INSERT COMPLETELY NEW CONTENT
--
-- A content item is considered new only when NO historical
-- record exists for the content_id.
-- ===========================================================

INSERT INTO gold.dim_content
(
    content_id,

    title,
    content_type_name,
    primary_genre,
    release_year,

    effective_from,
    effective_to,
    is_current,

    created_at,
    updated_at,
    source_system
)

SELECT
    source.content_id,

    source.title,
    source.content_type_name,
    source.primary_genre,
    source.release_year,

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

FROM silver.content_profile AS source

WHERE NOT EXISTS
(
    SELECT 1

    FROM gold.dim_content AS existing

    WHERE existing.content_id = source.content_id
)

AND COALESCE(
        source.created_at,
        source.updated_at
    ) IS NOT NULL;


-- ===========================================================
-- STEP 3
-- INSERT NEW SCD TYPE 2 VERSION
--
-- The previous version has already been closed by STEP 1.
--
-- The existence of a historical record confirms that this
-- content item already existed.
-- ===========================================================

INSERT INTO gold.dim_content
(
    content_id,

    title,
    content_type_name,
    primary_genre,
    release_year,

    effective_from,
    effective_to,
    is_current,

    created_at,
    updated_at,
    source_system
)

SELECT
    source.content_id,

    source.title,
    source.content_type_name,
    source.primary_genre,
    source.release_year,

    source.updated_at AS effective_from,

    TIMESTAMP '9999-12-31 23:59:59'
        AS effective_to,

    TRUE AS is_current,

    source.created_at,
    source.updated_at,
    source.source_system

FROM silver.content_profile AS source

WHERE source.updated_at IS NOT NULL


  -- ---------------------------------------------------------
  -- Content item must already exist historically.
  -- ---------------------------------------------------------

  AND EXISTS
  (
      SELECT 1

      FROM gold.dim_content AS historical

      WHERE historical.content_id = source.content_id
  )


  -- ---------------------------------------------------------
  -- There must not already be a current version.
  --
  -- STEP 1 closes the previous version when a real change
  -- occurs.
  -- ---------------------------------------------------------

  AND NOT EXISTS
  (
      SELECT 1

      FROM gold.dim_content AS current_version

      WHERE current_version.content_id = source.content_id

        AND current_version.is_current = TRUE
  )


  -- ---------------------------------------------------------
  -- Confirm that STEP 1 actually closed the previous version
  -- at this source change timestamp.
  -- ---------------------------------------------------------

  AND EXISTS
  (
      SELECT 1

      FROM gold.dim_content AS previous_version

      WHERE previous_version.content_id = source.content_id

        AND previous_version.is_current = FALSE

        AND previous_version.effective_to =
            source.updated_at
  );