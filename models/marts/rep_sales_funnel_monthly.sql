{{ config(materialized='table') }}

WITH raw_changes AS (
    -- Extract Stage Moves and Deal Creations
    SELECT 
        deal_id,
        CAST(occurred_at AS DATE) AS event_date,
        field_key,
        new_value
    FROM {{ ref('stg_crm__deal_changes') }}
    WHERE field_key IN ('stage_id', 'add_time')
),

raw_activities AS (
    -- Extract Completed Sales Calls
    SELECT 
        deal_id,
        CAST(due_to AS DATE) AS event_date, -- Using due_to as the event date
        type
    FROM {{ source('crm', 'activity') }} -- Direct source reference for demo
    WHERE done = TRUE 
      AND type IN ('meeting', 'sc_2')
),

-- Standardize everything into a "Funnel Event" stream
funnel_events AS (
    
    -- 1. Handle Deal Creation (Step 1)
    SELECT 
        deal_id,
        event_date,
        1 AS funnel_step,
        'Lead Generation' AS kpi_name
    FROM raw_changes
    WHERE field_key = 'add_time'

    UNION ALL

    -- 2. Handle Standard Stage Moves (Steps 2, 4, 6-11)
    SELECT 
        deal_id,
        event_date,
        CASE CAST(new_value AS INTEGER)
            WHEN 2 THEN 2
            WHEN 3 THEN 4  -- Skip 3 (Call 1)
            WHEN 4 THEN 6  -- Skip 5 (Call 2)
            WHEN 5 THEN 7
            WHEN 6 THEN 8
            WHEN 7 THEN 9
            WHEN 8 THEN 10
            WHEN 9 THEN 11
        END AS funnel_step,
        CASE CAST(new_value AS INTEGER)
            WHEN 2 THEN 'Qualified Lead'
            WHEN 3 THEN 'Needs Assessment'
            WHEN 4 THEN 'Proposal/Quote Preparation'
            WHEN 5 THEN 'Negotiation'
            WHEN 6 THEN 'Closing'
            WHEN 7 THEN 'Implementation/Onboarding'
            WHEN 8 THEN 'Follow-up/Customer Success'
            WHEN 9 THEN 'Renewal/Expansion'
        END AS kpi_name
    FROM raw_changes
    WHERE field_key = 'stage_id'
      AND CAST(new_value AS INTEGER) BETWEEN 2 AND 9

    UNION ALL

    -- 3. Handle Activity Steps (Steps 3 and 5)
    SELECT 
        deal_id,
        event_date,
        CASE type
            WHEN 'meeting' THEN 3
            WHEN 'sc_2' THEN 5
        END AS funnel_step,
        CASE type
            WHEN 'meeting' THEN 'Sales Call 1'
            WHEN 'sc_2' THEN 'Sales Call 2'
        END AS kpi_name
    FROM raw_activities

),

-- Aggregation
monthly_stats AS (
    SELECT
        -- Format Month as YYYY-MM
        TO_CHAR(DATE_TRUNC('month', event_date), 'YYYY-MM') AS month,
        kpi_name,
        funnel_step,
        COUNT(DISTINCT deal_id) AS deals_count
    FROM funnel_events
    WHERE funnel_step IS NOT NULL -- Safety filter
    GROUP BY 1, 2, 3
)

SELECT * FROM monthly_stats
ORDER BY month DESC, funnel_step ASC