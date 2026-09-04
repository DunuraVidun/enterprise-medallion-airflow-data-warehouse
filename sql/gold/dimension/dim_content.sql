-- ===========================================================
-- GOLD DIM CONTENT
-- SCD TYPE 2
-- ===========================================================


-- ===========================================================
-- STEP 1
-- Close existing current records when content attributes
-- have changed.
-- ===========================================================

UPDATE gold.dim_content AS target

SET
    effective_to = CURRENT_TIMESTAMP,
    is_current = FALSE,
    updated_at = CURRENT_TIMESTAMP

FROM silver.content_profile AS source

WHERE target.content_id = source.content_id

  AND target.is_current = TRUE

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
-- Insert:
--
-- 1. Completely new content items
-- 2. New versions of content items whose previous version
--    was closed in STEP 1
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

    CURRENT_TIMESTAMP AS effective_from,

    TIMESTAMP '9999-12-31 23:59:59'
        AS effective_to,

    TRUE AS is_current,

    source.created_at,
    source.updated_at,
    source.source_system

FROM silver.content_profile AS source

LEFT JOIN gold.dim_content AS target

    ON target.content_id = source.content_id

    AND target.is_current = TRUE

WHERE target.content_sk IS NULL;