{{
    config(
        materialized='incremental',
        unique_key='ticket_id',
        incremental_strategy='delete+insert'
    )
}}

SELECT
    ticket_id,
    requester_email,
    requester_type,
    investor_id,
    rm_id,
    resolved_partner_id,
    status,
    priority,
    created_at,
    resolved_at,
    tags,
    
    -- Boolean flags for easy filtering/counting in BI tools
    resolved_at IS NOT NULL AS is_resolved,
    priority = 'urgent' AS is_urgent,
    
    -- Suggestion: Calculate hours as well for more granular resolution analysis
    DATEDIFF('DAY', created_at, resolved_at) AS time_to_resolution_days,
    DATEDIFF('HOUR', created_at, resolved_at) AS time_to_resolution_hours,
    -- (This can be further enriched in a join model)
    CASE 
        WHEN status IN ('resolved', 'closed') THEN TRUE 
        ELSE FALSE 
    END AS is_completed

FROM 
    {{ ref('int_ticket_requester_resolution') }}

{% if is_incremental() %}
WHERE created_at >= (
    SELECT DATEADD('DAY', -3, MAX(created_at))
    FROM {{ this }}
)
{% endif %}