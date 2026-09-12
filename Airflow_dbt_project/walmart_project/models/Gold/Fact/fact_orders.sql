SELECT 
    order_id,
    customer_id,
    product_id,
    order_item_id,
    employee_id,
    total_amount,
    quantity,
    unit_price,
    line_amount
FROM
    {{ ref('obt_b') }}