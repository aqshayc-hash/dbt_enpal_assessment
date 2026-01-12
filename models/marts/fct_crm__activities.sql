{{ config(
    materialized='incremental',
    unique_key='activity_id'
) }}

with activities as (
    -- This relies on the "Platform" version of int_deal_activities
    select * from {{ ref('int_deal_activities') }}
    {% if is_incremental() %}
    where activity_at >= (select max(activity_at) from {{ this }})
    {% endif %}
)

select
    activity_id,
    deal_id,
    user_id,
    activity_type_key as activity_type, -- raw key 
    activity_type_name as kpi_name,      -- human-readable name 
    is_done,
    activity_at
from activities