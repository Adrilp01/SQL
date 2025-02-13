--Ejercicio 8

set @@dataset_id = 'keepcoding';

with flag as (
  select
    calls_ivr_id,
    module_name,
    case when module_name in('AVERIA_MASIVA') then 1
    else 0
    end as masiva_lg
  from ivr_detail
)

select 
  calls_ivr_id,
  module_name,
  masiva_lg
from flag
qualify row_number() over( partition by cast(calls_ivr_id as string) order by module_name) = 1;