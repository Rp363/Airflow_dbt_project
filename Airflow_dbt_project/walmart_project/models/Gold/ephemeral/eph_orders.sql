SELECT 
    Distinct
    order_id,
    order_item_id,
    payment_method,
    order_timestamp,
    order_status,
    order_created_timestamp,
    order_updated_timestamp,
    order_is_active,
    order_processed_at,
    obt_processed_at,
    current_timestamp() as orders_gold_processed_at
FROM
    {{ ref('obt_b') }} 