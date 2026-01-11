{{ config(materialized='table') }}

with stage_events as (
    select 
        deal_id, 
        valid_from, 
        funnel_step_number, 
        kpi_name 
    from {{ ref('int_deal_stage_history') }}
),

activity_events as (
    select 
        deal_id, 
        valid_from, 
        funnel_step_number, 
        kpi_name 
    from {{ ref('int_deal_activities') }}
),

-- Combine both streams
all_events as (
    select * from stage_events
    union all
    select * from activity_events
),

-- Aggregate
monthly_stats as (
    select
        -- Robust Date Truncation
        date_trunc('month', valid_from) as report_month,
        kpi_name,
        cast(funnel_step_number as numeric) as funnel_step,
        count(distinct deal_id) as deals_count
    from all_events
    group by 1, 2, 3
)

select 
    to_char(report_month, 'YYYY-MM') as month,
    kpi_name,
    funnel_step,
    deals_count
from monthly_stats
order by report_month desc, funnel_step asc