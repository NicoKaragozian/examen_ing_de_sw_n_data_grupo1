-- Test: La suma de montos en staging debe ser igual a la suma de total_amount_all en mart
-- Justificación: Valida la integridad de datos entre capas - no debe perderse ni
-- duplicarse información durante la agregación.

with totales_staging as (
    select
        sum(amount) as total_staging
    from {{ ref('stg_transactions') }}
),

totales_mart as (
    select
        sum(total_amount_all) as total_mart
    from {{ ref('fct_customer_transactions') }}
)

select
    totales_staging.total_staging,
    totales_mart.total_mart,
    abs(totales_staging.total_staging - totales_mart.total_mart) as diferencia
from totales_staging
cross join totales_mart
where abs(totales_staging.total_staging - totales_mart.total_mart) > 0.01
