--  Ejercicio 3

set @@dataset_id = 'keepcoding';

create table ivr_detail as
select
    cal.ivr_id as calls_ivr_id,
    cal.phone_number as calls_phone_number,
    cal.ivr_result as calls_ivr_result,
    cal.vdn_label as calls_vdn_label,
    cast(format_timestamp('%Y%m%d', cal.start_date) as int64) as calls_start_date,
    row_number() over (order by cal.start_date) as calls_start_date_id,
    cast(format_timestamp('%Y%m%d', cal.end_date) as int64) as calls_end_date,
    row_number() over (order by cal.end_date) as calls_end_date_id,
    cal.total_duration as calls_total_duration,
    cal.customer_segment as calls_customer_segment,
    cal.ivr_language as calls_ivr_language,
    cal.steps_module as calls_steps_module,
    cal.module_aggregation as calls_module_aggregation,
    mod.module_sequece,
    mod.module_name,
    mod.module_duration,
    mod.module_result,
    ste.step_sequence,
    ste.step_name,
    ste.step_result,
    ste.step_description_error,
    ste.document_type,
    ste.document_identification,
    ste.customer_phone,
    ste.billing_account_id
from keepcoding.ivr_calls cal
left join keepcoding.ivr_modules mod
    on cal.ivr_id = mod.ivr_id
left join keepcoding.ivr_steps ste
    on cal.ivr_id = ste.ivr_id
    and mod.module_sequece = ste.module_sequece;

