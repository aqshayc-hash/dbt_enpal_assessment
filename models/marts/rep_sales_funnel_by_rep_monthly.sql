{{ config(materialized='table') }}

with stage_events as (
    select 
        deal_id,
        valid_from,
        rep_name,
        funnel_step_number,
        kpi_name
    from {{ ref('int_deal_stage_history') }}
),

activity_events as (
    select 
        deal_id,
        activity_at as valid_from,
        rep_name,
        case 
            when activity_type_key = 'meeting' then 2.1
            when activity_type_key = 'sc_2' then 3.1
        end as funnel_step_number,
        activity_type_name as kpi_name
    from {{ ref('int_deal_activities') }}
    where activity_type_key in ('meeting', 'sc_2')
      and is_done = true
),

all_events as (
    select * from stage_events
    union all
    select * from activity_events
),

monthly_rep_stats as (
    select
        date_trunc('month', valid_from) as report_month,
        -- Coalesce nulls to ensure we don't lose data
        coalesce(rep_name, 'Unassigned') as rep_name,
        kpi_name,
        funnel_step_number as funnel_step,
        count(distinct deal_id) as deals_count
    from all_events
    group by 1, 2, 3, 4
)

select 
    to_char(report_month, 'YYYY-MM') as month,
    rep_name,
    kpi_name,
    funnel_step,
    deals_count
from monthly_rep_stats
order by report_month desc, rep_name asc, funnel_step asc