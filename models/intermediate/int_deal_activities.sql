with activities as (
    select * from {{ ref('stg_crm__activities') }}
),

mapped as (
    select
        deal_id,
        due_at as valid_from,
        
        -- Map Activity Type to Funnel Step
        case activity_type
            when 'meeting' then 3
            when 'sc_2' then 5
        end as funnel_step_number,

        case activity_type
            when 'meeting' then 'Sales Call 1'
            when 'sc_2' then 'Sales Call 2'
        end as kpi_name

    from activities
    where is_done = true
      and activity_type in ('meeting', 'sc_2')
)

select * from mapped
where funnel_step_number is not null