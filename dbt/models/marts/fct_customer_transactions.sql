{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('stg_transactions') }}
),

per_customer as (
    select
        customer_id,
        count(*) as transaction_count,
        sum(case when status = 'completed' then amount else 0 end)
            as total_amount_completed,
        sum(amount) as total_amount_all
    from base
    group by customer_id
)

select
    customer_id,
    transaction_count,
    total_amount_completed,
    total_amount_all
from per_customer

