{{ config(materialized='table') }}

with users as (
    select * from {{ ref('stg_crm__users') }}
)

select
    user_id,
    user_name,
    email,
    updated_at,
    -- Simple logic to handle deactivated users if 'active_flag' existed
    true as is_active 
from users