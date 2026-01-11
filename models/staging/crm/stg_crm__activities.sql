with source as (
    select * from {{ source('crm', 'activity') }}
),

activity_renamed as (
    select
        cast(activity_id as integer) as activity_id,
        cast(deal_id as integer) as deal_id,
        cast(assigned_to_user as integer) as user_id,
        cast(type as varchar) as activity_type,
        -- Standardize Boolean
        cast(done as boolean) as is_done,
        -- Standardize Timestamp
        cast(due_to as timestamp) as due_at
    from source
)

select * from activity_renamed