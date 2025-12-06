


with source as (
    select *
    from read_parquet(
        '/Users/nicolasvelazquez/documents/udesa/ingenieriadatos/examen_ing_de_sw_n_data_final/data/clean/transactions_20251201_clean.parquet'
    )
)

select
    transaction_id,
    customer_id,
    transaction_date as transaction_ts, -- EL TEST PIDE transaction_ts
    transaction_date,                   -- Mantenemos la original por si acaso
    amount,
    status
from source