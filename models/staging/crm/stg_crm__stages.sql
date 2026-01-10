select 
    cast(stage_id as integer) as stage_id,
    cast(stage_name as varchar) as stage_name,
    -- Create a sort order based on ID
    cast(stage_id as integer) as stage_order
from {{ source('crm', 'stages') }}