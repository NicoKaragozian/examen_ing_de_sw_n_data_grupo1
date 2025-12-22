-- Test: Cada cliente en fct_customer_transactions debe tener al menos una transacción
-- Justificación: Si un cliente aparece en la tabla agregada, debe haber realizado
-- al menos una transacción. Un conteo de 0 indicaría un problema de datos.

select
    customer_id,
    transaction_count
from {{ ref('fct_customer_transactions') }}
where transaction_count < 1
