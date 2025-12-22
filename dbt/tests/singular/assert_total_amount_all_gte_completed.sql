-- Test: total_amount_all siempre debe ser >= total_amount_completed
-- Justificación: La suma de todas las transacciones nunca puede ser menor
-- que la suma de solo las transacciones completadas.

select
    customer_id,
    total_amount_all,
    total_amount_completed
from {{ ref('fct_customer_transactions') }}
where total_amount_all < total_amount_completed
