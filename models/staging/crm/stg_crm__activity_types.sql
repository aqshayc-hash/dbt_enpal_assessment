select
    cast(id as integer) as activity_type_id,
    cast(type as varchar) as activity_type_key, -- e.g. 'sc_2'
    cast(name as varchar) as activity_type_name,    -- e.g. 'Sales Call 2'
    cast(active as boolean) as is_active
from {{ source('crm', 'activity_types') }}