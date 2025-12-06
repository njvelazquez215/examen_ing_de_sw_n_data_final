with transactions as (
    select * from "medallion"."main"."stg_transactions"
),

customer_stats as (
    select
        customer_id,
        
        -- Métrica 1: Cantidad de transacciones
        count(transaction_id) as transaction_count,
        
        -- Métrica 2: Monto total (todos los estados)
        sum(amount) as total_amount_all,
        
        -- Métrica 3: Monto solo de completados (logica condicional)
        sum(case when status = 'completed' then amount else 0 end) as total_amount_completed,
        
        -- Métrica 4: Última fecha
        max(transaction_ts) as last_transaction_date

    from transactions
    group by customer_id
)

select * from customer_stats