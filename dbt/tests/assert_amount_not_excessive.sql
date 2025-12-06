-- Este test falla si encuentra transacciones con montos sospechosamente altos
select
    transaction_id,
    amount
from {{ ref('stg_transactions') }}
where amount > 100000
