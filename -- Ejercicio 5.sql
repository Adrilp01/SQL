-- Ejercicio 5
set @@dataset_id = 'keepcoding';

select 
    calls_ivr_id, 
    document_type,
    document_identification
from ivr_detail
qualify row_number() over (partition by cast(calls_ivr_id as string) order by document_type asc) = 1;
