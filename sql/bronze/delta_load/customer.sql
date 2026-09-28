SELECT
    *,
    CURRENT_TIMESTAMP AS load_timestamp,
    'ABC_HUB_OLTP' AS source_system
FROM customer_mgmt.customer
WHERE GREATEST(created_at, updated_at) > '{{ last_watermark }}'
AND GREATEST(created_at, updated_at) <= '{{ processing_date }}';