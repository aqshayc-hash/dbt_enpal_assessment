with source as (
    select * from {{ source('crm', 'deal_changes') }}
),

deal_changes_renamed as (
    select
        -- Create a surrogate key because raw table has no PK
        {{ dbt_utils.generate_surrogate_key(['deal_id', 'change_time', 'changed_field_key', 'new_value']) }} as change_id,
        cast(deal_id as integer) as deal_id,
        cast(change_time as timestamp) as occurred_at,
        cast(changed_field_key as varchar) as field_key,
        cast(new_value as varchar) as new_value
    from source
)

select * from deal_changes_renamed