--Ejercicio 7
set @@dataset_id = 'keepcoding';

select
  calls_ivr_id,
  billing_account_id
from ivr_detail
qualify row_number() over (partition by cast(calls_ivr_id as string) order by billing_account_id) = 1;