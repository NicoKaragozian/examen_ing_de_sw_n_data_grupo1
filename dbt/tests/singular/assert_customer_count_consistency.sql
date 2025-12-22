-- Test: El número de clientes únicos en staging debe coincidir con mart
-- Justificación: Cada cliente con transacciones en staging debe aparecer exactamente
-- una vez en la tabla agregada del mart.

with clientes_staging as (
    select count(distinct customer_id) as cantidad_clientes
    from {{ ref('stg_transactions') }}
),

clientes_mart as (
    select count(*) as cantidad_clientes
    from {{ ref('fct_customer_transactions') }}
)

select
    clientes_staging.cantidad_clientes as clientes_en_staging,
    clientes_mart.cantidad_clientes as clientes_en_mart
from clientes_staging
cross join clientes_mart
where clientes_staging.cantidad_clientes != clientes_mart.cantidad_clientes
