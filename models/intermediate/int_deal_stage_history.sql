with changes as (
    select * from {{ ref('stg_crm__deal_changes') }}
),

stages as (
    select * from {{ ref('stg_crm__stages') }}
),

-- 1. Extract Deal Creation (This acts as Stage 1 Entry usually)
creations as (
    select
        deal_id,
        occurred_at as valid_from,
        1 as stage_id -- Defaulting creation to Stage 1 (Lead Gen)
    from changes
    where field_key = 'add_time'
),

-- 2. Extract Stage Moves
moves as (
    select
        deal_id,
        occurred_at as valid_from,
        cast(new_value as integer) as stage_id
    from changes
    where field_key = 'stage_id'
),

-- 3. Extract Lost Events
losses as (
    select
        deal_id,
        occurred_at as valid_from,
        0 as stage_id -- We use 0 or a specific ID for Lost
    from changes
    where field_key = 'lost_reason'
),

combined_events as (
    select * from creations
    union all
    select * from moves
    union all 
    select * from losses
),

final as (
    select
        e.deal_id,
        e.valid_from,
        -- Truncate to month for reporting
        date_trunc('month', e.valid_from) as report_month,
        e.stage_id,
        s.stage_name,
        s.stage_order
    from combined_events e
    left join stages s on e.stage_id = s.stage_id
    -- Determine if this is the "Lost" stage manually if stage_id is 0
    where e.stage_id != 0 
)

select * from final