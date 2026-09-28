SELECT
    *,
    CURRENT_TIMESTAMP AS load_timestamp,
    'ABC_HUB_OLTP' AS source_system
FROM content_mgmt.genre
WHERE GREATEST(created_at, updated_at) > '{{ last_watermark }}'
AND GREATEST(created_at, updated_at) <= '{{ processing_date }}';