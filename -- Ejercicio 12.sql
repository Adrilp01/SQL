-- Ejercicio 12
set @@dataset_id = 'keepcoding';


create table ivr_summary as

with vdn_agreggation as (
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
qualify row_number() over(partition by cast(calls_ivr_id as string) order by calls_ivr_id) = 1
),

document_type as (
select 
    calls_ivr_id, 
    document_type
from ivr_detail
qualify row_number() over (partition by cast(calls_ivr_id as string) order by document_type asc) = 1
),

document_identification as (
select 
    calls_ivr_id,
    document_identification
from ivr_detail
qualify row_number() over (partition by cast(calls_ivr_id as string) order by document_type asc) = 1
),

customer_phone as (
  select
  calls_ivr_id,
  customer_phone
from ivr_detail
qualify row_number() over (partition by cast (calls_ivr_id as string) order by customer_phone) = 1
),

billing_account_id as (
  select
  calls_ivr_id,
  billing_account_id
from ivr_detail
qualify row_number() over (partition by cast(calls_ivr_id as string) order by billing_account_id) = 1
),

masiva_lg as (
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
qualify row_number() over( partition by cast(calls_ivr_id as string) order by module_name) = 1
),

info_by_phone_lg as (with flag as (
  select
    calls_ivr_id,
    step_name,
    step_result,
    case when step_name in('CUSTOMERINFOBYPHONE.TX') and step_result in('OK') then 1
    else 0
    end as info_by_phone_lg
  from ivr_detail
)

select 
  calls_ivr_id,
  step_name,
  step_result,
  info_by_phone_lg
from flag
qualify row_number() over(partition by cast(calls_ivr_id as string) order by info_by_phone_lg desc) = 1
),

info_by_dni_lg as (with flag as (
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
qualify row_number() over(partition by cast(calls_ivr_id as string) order by info_by_dni_lg desc) = 1
),

repeated_phone_24H as (

with past_24h as (
select
  calls_ivr_id,
  calls_phone_number,
  case
  when date_add(parse_date('%Y%m%d', cast(lag(calls_start_date) over (partition by calls_phone_number order by calls_start_date) as string)), interval 1 DAY) = parse_date('%Y%m%d', cast(calls_start_date as string)) then 1
  else 0
  end as repeated_phone_24H
  from ivr_detail)

select
  det.calls_ivr_id,
  det.calls_phone_number,
  det.calls_start_date,
  pas.repeated_phone_24H
from ivr_detail det
left join past_24h pas
on det.calls_ivr_id = pas.calls_ivr_id
qualify row_number() over(partition by cast(calls_ivr_id as string) order by calls_phone_number desc) = 1
),

cause_recall_phone_24H as (

  with next_24h as (
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
  nex.cause_recall_phone_24H
from ivr_detail det
left join next_24h nex
on det.calls_ivr_id = nex.calls_ivr_id
qualify row_number() over(partition by cast(calls_ivr_id as string) order by calls_phone_number desc) = 1
),

ivr_summary as(
select
  det.calls_ivr_id,
  det.calls_phone_number,
  det.calls_ivr_result,
  vdn_aggregation,
  det.calls_start_date,
  det.calls_end_date,
  det.calls_total_duration,
  det.calls_customer_segment,
  det.calls_ivr_language,
  det.calls_module_aggregation,
  dty.document_type,
  did.document_identification, 
  cus.customer_phone, 
  bil.billing_account_id, 
  mas.masiva_lg,
  iph.info_by_phone_lg,
  idn.info_by_dni_lg, 
  rep.repeated_phone_24H,
  cre.cause_recall_phone_24H
from ivr_detail det
left join vdn_agreggation vdn
on vdn.calls_ivr_id = det.calls_ivr_id
left join document_type dty
on dty.calls_ivr_id = det.calls_ivr_id
left join document_identification did
on did.calls_ivr_id = det.calls_ivr_id
left join customer_phone cus
on cus.calls_ivr_id = det.calls_ivr_id
left join billing_account_id bil
on bil.calls_ivr_id = det.calls_ivr_id
left join masiva_lg mas
on mas.calls_ivr_id = det.calls_ivr_id
left join info_by_phone_lg iph
on iph.calls_ivr_id = det.calls_ivr_id
left join info_by_dni_lg idn
on idn.calls_ivr_id = det.calls_ivr_id
left join repeated_phone_24H rep
on rep.calls_ivr_id = det.calls_ivr_id
left join cause_recall_phone_24H cre
on cre.calls_ivr_id = det.calls_ivr_id
)

select *
from ivr_summary