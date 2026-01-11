with activities as (
    select * from {{ ref('stg_crm__activities') }}
),

-- Join to TYPES to make it robust against naming changes
types as (
    select * from {{ ref('stg_crm__activity_types') }} 
),

users as (
    select * from {{ ref('stg_crm__users') }}
),

enriched as (
    select
        a.deal_id,
        a.activity_id,
        a.due_at as activity_at,
        a.is_done,
        
        -- Dimensions
        a.user_id,
        u.user_name as rep_name,
        
        -- Type Resolution
        t.activity_type_name, -- 'Sales Call 2'
        t.activity_type_key,  -- 'sc_2'

        -- Platform Logic: We create generic "categories" here, 
        -- but we do NOT filter rows out.
        case 
            when t.activity_type_key = 'meeting' then 'Funnel Step 2.1'
            when t.activity_type_key = 'sc_2' then 'Funnel Step 3.1'
            else 'Other' 
        end as funnel_mapping_category

    from activities a
    left join types t on a.activity_type = t.activity_type_key
    left join users u on a.user_id = u.user_id
)

select * from enriched
-- NOTICE: No "WHERE" clause filtering specific types. 
-- This table now serves ALL activity reports.