WITH tickets AS (
    
    SELECT *
    FROM {{ ref('int_ticket_requester_resolution') }}
    WHERE requester_type = 'investor'

),

agg AS (

    SELECT
        investor_id,
        COUNT(*) AS total_tickets,
        SUM(CASE WHEN priority = 'urgent' THEN 1 ELSE 0 END) AS urgent_tickets,
        AVG(DATEDIFF('DAY', created_at, resolved_at)) AS avg_ttr,
        MAX(created_at) AS last_ticket_at
    FROM tickets
    GROUP BY investor_id

),

investors AS (

    SELECT
        i.investor_id,
        i.full_name,
        i.email,
        i.entity_id,
        e.partner_id,
        i.relationship_manager_id,
        i.country,
        i.created_at AS investor_created_at
    FROM {{ ref('stg_platform_investors') }} i
    LEFT JOIN {{ ref('stg_platform_entities') }} e
        ON i.entity_id = e.entity_id

),

final_joined AS (

    SELECT
        i.investor_id,
        i.full_name,
        i.email,
        i.entity_id,
        i.partner_id,
        i.relationship_manager_id,
        i.country,
        i.investor_created_at,
        COALESCE(a.total_tickets, 0) AS total_tickets,
        COALESCE(a.urgent_tickets, 0) AS urgent_tickets,
        a.avg_ttr,
        a.last_ticket_at,
        DATEDIFF('DAY', a.last_ticket_at, CURRENT_DATE) AS days_since_last_ticket
    FROM investors i
    LEFT JOIN agg a
        ON i.investor_id = a.investor_id

)

SELECT
    -- Investor Identifiers
    investor_id,
    full_name,
    email,
    entity_id,
    partner_id,
    relationship_manager_id,
    country,
    investor_created_at,
    
    -- Support Volume & Speed
    total_tickets,
    urgent_tickets,
    avg_ttr,
    last_ticket_at,
    days_since_last_ticket,
    
    -- Support Tiering Logic
    CASE 
        WHEN total_tickets >= 10 THEN 'high_touch'
        WHEN total_tickets BETWEEN 3 AND 9 THEN 'regular'
        WHEN total_tickets BETWEEN 1 AND 2 THEN 'low_touch'
        ELSE 'no_support_contact'
    END AS support_tier,

    -- Risk & Frustration Logic
    CASE 
        WHEN urgent_tickets > 0 AND avg_ttr > 7 THEN 'high_risk'
        WHEN urgent_tickets > 2 THEN 'elevated'
        ELSE 'stable'
    END AS frustration_status,

    -- Engagement Logic
    CASE 
        WHEN days_since_last_ticket > 90 THEN 'dormant'
        WHEN days_since_last_ticket <= 90 THEN 'active'
        ELSE 'never_contacted'
    END AS engagement_status
FROM 
    final_joined