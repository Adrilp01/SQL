--Ejercicio 6
set @@dataset_id = 'keepcoding';

select
  calls_ivr_id,
  customer_phone
from ivr_detail
qualify row_number() over (partition by cast (calls_ivr_id as string) order by customer_phone) = 1;