{{ config(
    materialized='incremental',
    unique_key=['deal_id', 'valid_from']
) }}

with history as (
    select * from {{ ref('int_deal_stage_history') }}
    {% if is_incremental() %}
    where valid_from >= (select max(valid_from) from {{ this }})
    {% endif %}
)

select
    deal_id,
    valid_from,
    funnel_step_number,
    kpi_name as stage_name,
    -- The rep who owned the deal *at this specific time*
    rep_name as historical_owner_name
from history