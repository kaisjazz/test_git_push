
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/



with source_data as (

    select 5 as id,CURRENT_TIMESTAMP() as date_load
    union all
    select 1 as id,CURRENT_TIMESTAMP() as date_load

) 

select *
from source_data

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  -- (uses >= to include records arriving later on the same day as the last run of this model)
  where CURRENT_TIMESTAMP() > (select max(date_load) from {{ this }})

{% endif %}





