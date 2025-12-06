
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Este test falla si encuentra transacciones con montos sospechosamente altos
select
    transaction_id,
    amount
from "medallion"."main"."stg_transactions"
where amount > 100000
  
  
      
    ) dbt_internal_test