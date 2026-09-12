{{ config(severity = 'warn') }}

SELECT 1
from 
    {{ ref('obt_b') }} as obt_b
where 
    obt_b.order_id is null
OR
    obt_b.product_id is null
OR
    obt_b.employee_id is null
OR
    obt_b.store_id is null
OR
    obt_b.order_item_id is null
OR
    obt_b.customer_id is null