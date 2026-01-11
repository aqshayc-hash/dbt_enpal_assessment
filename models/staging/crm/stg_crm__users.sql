with source as (
    select * from {{ source('crm', 'users') }}
),

users_renamed as (
    select
        cast(id as integer) as user_id,
        cast(name as varchar) as user_name,
        cast(email as varchar) as email,
        cast(modified as timestamp) as updated_at
    from source
)

select * from users_renamed