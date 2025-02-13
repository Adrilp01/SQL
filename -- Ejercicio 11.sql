-- Ejercicio 11

set @@dataset_id = 'keepcoding';
--anotación: si el qualify row_number() lo ordeno por calls_phone_number desc tengo bien los resultados de repeated_phone
-- al hacer el row_number algunos valores con unos1 en los flags se me quedan en filas random, no consigo ordenarlas para que se queden las primeras, creo que por eso no recibo apenas valores de cause_recall_phone. 
--Si aplico el where que he dejado al final si me aparecen 1 en el flag cause_recall, pero al no aplicarlo y buscar los mismos id no aparecen los unos1.


with past_24h as (
select
  calls_ivr_id,
  calls_phone_number,
  case
  when date_add(parse_date('%Y%m%d', cast(lag(calls_start_date) over (partition by calls_phone_number order by calls_start_date) as string)), interval 1 DAY) = parse_date('%Y%m%d', cast(calls_start_date as string)) then 1
  else 0
  end as repeated_phone_24H
  from ivr_detail),

next_24h as (
select
  calls_ivr_id,
  calls_phone_number,
  case 
  when date_add(parse_date('%Y%m%d', cast(lead(calls_start_date) over (partition by  calls_phone_number order by calls_start_date) as string)), interval -1 DAY) = parse_date('%Y%m%d', cast(calls_start_date as string)) then 1
  else 0
  end as cause_recall_phone_24H
  from ivr_detail)

select
  det.calls_ivr_id,
  det.calls_phone_number,
  det.calls_start_date,
  pas.repeated_phone_24H,
  nex.cause_recall_phone_24H
from ivr_detail det
left join past_24h pas
on det.calls_ivr_id = pas.calls_ivr_id
left join next_24h nex
on det.calls_ivr_id = nex.calls_ivr_id
--where cause_recall_phone_24H = 1
qualify row_number() over(partition by cast(calls_ivr_id as string) order by calls_phone_number desc) = 1