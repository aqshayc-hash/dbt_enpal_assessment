select
    cast(id as integer) as activity_type_id,
    cast(type as varchar) as activity_type_key, 
    cast(name as varchar) as activity_type_name,    
    cast(active as boolean) as is_active
from {{ source('crm', 'activity_types') }}