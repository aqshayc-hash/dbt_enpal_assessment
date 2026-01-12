{{ config(materialized='table') }}

with stage_facts as (
    select 
        deal_id,
        valid_from,
        stage_name as kpi_name,
        funnel_step_number as funnel_step
    from {{ ref('fct_crm__deal_history') }}
),

activity_facts as (
    select 
        deal_id,
        activity_at as valid_from,
        kpi_name,
        -- Report-specific Mapping: Activities
        case activity_type
            when 'meeting' then 2.1
            when 'sc_2' then 3.1
            else null -- Filter out irrelevant activities for THIS report
        end as funnel_step
    from {{ ref('fct_crm__activities') }}
    where is_done = true
),

union_facts as (
    select * from stage_facts
    union all
    select * from activity_facts
)

select
    -- Final Presentation Layer
    to_char(date_trunc('month', valid_from), 'YYYY-MM') as month,
    kpi_name,
    funnel_step,
    count(distinct deal_id) as deals_count
from union_facts
where funnel_step is not null -- Clean out non-funnel activities
group by 1, 2, 3
order by 1 desc, 3 asc