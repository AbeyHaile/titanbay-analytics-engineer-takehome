WITH tickets AS (

    SELECT
        ticket_id,
        resolved_partner_id,
        created_at,
        priority = 'urgent' AS is_urgent
    FROM {{ ref('int_ticket_requester_resolution') }}
    WHERE resolved_partner_id IS NOT NULL

),

closes AS (

    SELECT 
        c.close_id,
        c.partner_id,
        c.fund_name,
        c.scheduled_close_date,
        c.close_status,
        c.total_committed_aum,
        p.partner_name
    FROM {{ ref('stg_platform_fund_closes') }} c
    LEFT JOIN {{ ref('stg_platform_partners') }} p 
        ON c.partner_id = p.partner_id

),

joined AS (

    SELECT
        c.partner_id,
        c.partner_name,
        c.close_id,
        c.fund_name,
        c.scheduled_close_date,
        c.close_status,
        c.total_committed_aum,
        t.ticket_id,
        t.is_urgent,
        DATEDIFF('DAY', t.created_at, c.scheduled_close_date) AS days_to_close
    FROM closes c
    LEFT JOIN tickets t
        ON c.partner_id = t.resolved_partner_id
        AND DATEDIFF('DAY', t.created_at, c.scheduled_close_date) BETWEEN -7 AND 14

),

aggregated_metrics AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['partner_id','close_id']) }}
            AS partner_close_support_pressure_sk,
        partner_id,
        partner_name,
        close_id,
        fund_name,
        scheduled_close_date,
        close_status,
        MAX(total_committed_aum) AS total_committed_aum,

        COUNT(ticket_id) AS total_tickets_in_close_window,
        SUM(CASE WHEN is_urgent THEN 1 ELSE 0 END) AS urgent_tickets_in_close_window,

        COUNT(CASE WHEN days_to_close BETWEEN 8 AND 14 THEN ticket_id END) 
            AS tickets_8_to_14_days_before_close,
        COUNT(CASE WHEN days_to_close BETWEEN 0 AND 7 THEN ticket_id END) 
            AS tickets_0_to_7_days_before_close,
        COUNT(CASE WHEN days_to_close BETWEEN -7 AND -1 THEN ticket_id END) 
            AS tickets_1_to_7_days_after_close

    FROM joined
    GROUP BY 1, 2, 3, 4, 5, 6, 7

),

distribution_stats AS (

    SELECT 
        QUANTILE_CONT(total_committed_aum, 0.75) AS p75_aum,
        QUANTILE_CONT(tickets_0_to_7_days_before_close, 0.75) AS p75_close_window_tickets,
        QUANTILE_CONT(tickets_0_to_7_days_before_close, 0.50) AS p50_close_window_tickets
    FROM aggregated_metrics

)

SELECT
    m.*,

    CASE 
        WHEN m.total_committed_aum >= s.p75_aum THEN TRUE
        ELSE FALSE
    END AS is_high_aum_close,

    CASE 
        WHEN m.total_committed_aum >= s.p75_aum
            AND m.tickets_0_to_7_days_before_close >= 2
        THEN TRUE
        ELSE FALSE
    END AS is_high_risk_high_value_close

FROM aggregated_metrics m
CROSS JOIN distribution_stats s