--Ejercicio 4

set @@dataset_id = 'keepcoding';

select
  calls_ivr_id,
  calls_vdn_label,
  case
    when calls_vdn_label like 'ATC%' then 'FRONT'
    when calls_vdn_label like 'TECH%' then 'TECH'
    when calls_vdn_label = 'ABSORPTION' then 'ABSORPTION'
    else 'RESTO'
    end as vdn_aggregation
from ivr_detail
qualify row_number() over(partition by cast(calls_ivr_id as string) order by calls_ivr_id) = 1;
