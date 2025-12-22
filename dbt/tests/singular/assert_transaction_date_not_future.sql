-- Test: Las fechas de transacción no deben estar en el futuro
-- Justificación: Las transacciones no pueden ocurrir en el futuro. Esto podría
-- indicar problemas de calidad de datos o de zona horaria.

select
    transaction_id,
    transaction_date,
    current_date as fecha_actual
from {{ ref('stg_transactions') }}
where transaction_date > current_date
