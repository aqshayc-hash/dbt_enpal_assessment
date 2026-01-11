with changes as (
    select * from {{ ref('stg_crm__deal_changes') }}
),

stages as (
    select * from {{ ref('stg_crm__stages') }}
),

-- 1. Unify Creation and Moves
events as (
    -- Creation = Funnel Step 1
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

-- 2. Join and Map to Final Funnel Steps
final as (
    select
        e.deal_id,
        e.valid_from,
        s.stage_name as raw_stage_name,
        
        -- The Critical Mapping Logic (Source ID -> Reporting Step)
        -- Activities fill in sub-steps: 2.1 (Sales Call 1), 3.1 (Sales Call 2)
        case e.stage_id
            when 1 then 1  -- Lead Gen
            when 2 then 2  -- Qualified Lead (followed by 2.1 Sales Call 1)
            when 3 then 3  -- Needs Assessment (followed by 3.1 Sales Call 2)
            when 4 then 4  -- Proposal/Quote Preparation
            when 5 then 5  -- Negotiation
            when 6 then 6  -- Closing
            when 7 then 7  -- Implementation/Onboarding
            when 8 then 8  -- Follow-up/Customer Success
            when 9 then 9  -- Renewal/Expansion
        end as funnel_step_number,

        -- Ensure we use the official Stage Name
        s.stage_name as kpi_name
    from events e
    left join stages s on e.stage_id = s.stage_id
)

select * from final
where funnel_step_number is not null