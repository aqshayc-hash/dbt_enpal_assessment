{{ config(
    materialized='incremental',
    unique_key='activity_id'
) }}

with activities as (
    -- This relies on the "Platform" version of int_deal_activities
    -- which should NO LONGER filter for specific types.
    select * from {{ ref('int_deal_activities') }}
    {% if is_incremental() %}
    where activity_at >= (select max(activity_at) from {{ this }})
    {% endif %}
)

select
    activity_id,
    deal_id,
    user_id,
    activity_type_key as activity_type, -- raw key (e.g. 'sc_2', 'call')
    activity_type_name as kpi_name,      -- human name (e.g. 'Sales Call 2')
    is_done,
    activity_at
from activities