WITH ranked_genres AS
(
    SELECT
        cg.content_id,
        g.genre_name,

        ROW_NUMBER() OVER
        (
            PARTITION BY cg.content_id
            ORDER BY cg.genre_id
        ) AS rn

    FROM silver.content_genre AS cg

    INNER JOIN silver.genre AS g
        ON cg.genre_id = g.genre_id
)

SELECT
    c.content_id,

    c.title,

    ct.content_type AS content_type_name,

    rg.genre_name AS primary_genre,

    EXTRACT(
        YEAR FROM c.release_date
    )::INT AS release_year,

    c.created_at,

    c.updated_at,

    CURRENT_TIMESTAMP AS load_timestamp,

    c.source_system

FROM silver.content AS c

LEFT JOIN silver.content_type AS ct
    ON c.content_type_id = ct.content_type_id

LEFT JOIN ranked_genres AS rg
    ON c.content_id = rg.content_id
    AND rg.rn = 1;