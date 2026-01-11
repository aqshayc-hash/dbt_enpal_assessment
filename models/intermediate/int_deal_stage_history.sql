with changes as (
    select * from {{ ref('stg_crm__deal_changes') }}
),

stages as (
    select * from {{ ref('stg_crm__stages') }}
),

users as (
    select * from {{ ref('stg_crm__users') }}
),

-- 1. Build the history of Deal Ownership
owner_history as (
    select
        deal_id,
        -- Convert string value to integer for joining
        cast(new_value as integer) as owner_user_id,
        occurred_at as valid_from,
        -- The owner is valid until the next time 'user_id' changes for this deal
        lead(occurred_at, 1, '9999-12-31') over (partition by deal_id order by occurred_at) as valid_to
    from changes
    where field_key = 'user_id'
),

-- 2. Isolate Stage Events
stage_events as (
    -- Creation (Funnel Step 1)
    select
        deal_id,
        occurred_at as valid_from,
        1 as stage_id
    from changes 
    where field_key = 'add_time'

    union all

    -- Moves
    select
        deal_id,
        occurred_at as valid_from,
        cast(new_value as integer) as stage_id
    from changes 
    where field_key = 'stage_id'
),

-- 3. Join Events to Stage Names and the Active Owner
final as (
    select
        e.deal_id,
        e.valid_from,
        
        -- Find the user who owned the deal at the time of the event
        u.user_name as rep_name,

        -- Funnel Mapping
        case e.stage_id
            when 1 then 1.0  -- Lead Gen
            when 2 then 2.0  -- Qualified Lead
            when 3 then 3.0  -- Needs Assessment
            when 4 then 4.0  -- Proposal
            when 5 then 5.0  -- Negotiation
            when 6 then 6.0  -- Closing
            when 7 then 7.0  -- Implementation
            when 8 then 8.0  -- Follow-up
            when 9 then 9.0  -- Renewal
        end as funnel_step_number,

        s.stage_name as kpi_name
    from stage_events e
    left join stages s on e.stage_id = s.stage_id
    -- Join to owner history where the event happened during ownership window
    left join owner_history o 
        on e.deal_id = o.deal_id 
        and e.valid_from >= o.valid_from 
        and e.valid_from < o.valid_to
    left join users u on o.owner_user_id = u.user_id
)

select * from final
where funnel_step_number is not null