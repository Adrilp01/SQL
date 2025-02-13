-- Ejercicio 10

set @@dataset_id = 'keepcoding';

with flag as (
  select
    calls_ivr_id,
    step_name,
    step_result,
    case when step_name in('CUSTOMERINFOBYDNI.TX') and step_result in('OK') then 1
    else 0
    end as info_by_dni_lg
  from ivr_detail
)

select 
  calls_ivr_id,
  step_name,
  step_result,
  info_by_dni_lg
from flag
qualify row_number() over(partition by cast(calls_ivr_id as string) order by info_by_dni_lg desc) = 1;