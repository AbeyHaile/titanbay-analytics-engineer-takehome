WITH tickets AS (

    SELECT
        ticket_id,
        LOWER(TRIM(requester_email)) AS requester_email,
        requester_name,
        subject,
        status,
        priority,
        created_at,
        resolved_at,
        tags,
        LOWER(TRIM(partner_label)) AS partner_label
    FROM {{ ref('stg_freshdesk_tickets') }}
),

investors AS (

    SELECT
        i.investor_id,
        LOWER(TRIM(i.email)) AS email,
        i.entity_id,
        e.partner_id,
        p.partner_name
    FROM {{ ref('stg_platform_investors') }} i
    LEFT JOIN {{ ref('stg_platform_entities') }} e
        ON i.entity_id = e.entity_id
    LEFT JOIN {{ ref('stg_platform_partners') }} p
        ON e.partner_id = p.partner_id
),

rms AS (

    SELECT
        rm_id,
        LOWER(TRIM(email)) AS email,
        partner_id
    FROM {{ ref('stg_platform_relationship_managers') }}

)

SELECT
    t.*,
    i.partner_name,
    CASE
        WHEN i.investor_id IS NOT NULL THEN 'investor'
        WHEN r.rm_id IS NOT NULL THEN 'relationship_manager'
        ELSE 'unknown'
    END AS requester_type,
    i.partner_id,
    i.investor_id,
    r.rm_id,
    COALESCE(i.partner_id, r.partner_id) AS resolved_partner_id
FROM tickets t
LEFT JOIN investors i
    ON t.requester_email = i.email
LEFT JOIN rms r
    ON t.requester_email = r.email