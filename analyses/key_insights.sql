-- 1. Investor concentration (who drives support demand)
-- Insight: ~3% of investors generate ~53% of all support tickets
SELECT
    COUNT(*) AS high_touch_investors,
    COUNT(*) * 1.0 / COUNT(*) OVER () AS pct_investors,
    SUM(total_tickets) AS tickets,
    SUM(total_tickets) * 1.0 / SUM(SUM(total_tickets)) OVER () AS pct_tickets
FROM fact_investor_support_profile
WHERE total_tickets >= 10;


-- 2. Distribution of tickets per investor
-- Question: Is support demand evenly spread?
-- Insight: Most investors have 0-2 tickets, while a small long-tail group creates repeated support demand.
SELECT
    total_tickets,
    COUNT(*) AS investors
FROM fact_investor_support_profile
GROUP BY 1
ORDER BY 1 DESC;


-- 3. Close pressure distribution
-- Question: Do most fund close events create support pressure?
-- Insight: Only ~27% of fund close events generate any tickets in the final week before close.
SELECT
    COUNT(*) AS total_closes,
    COUNT(CASE WHEN tickets_0_to_7_days_before_close > 0 THEN 1 END) AS closes_with_tickets,
    AVG(tickets_0_to_7_days_before_close) AS avg_tickets_per_close
FROM fact_partner_close_pressure;


-- 4. High-risk, high-value closes
-- Question: Which fund close events should IS prioritise?
-- Insight: Only a small number of closes are both high-value and high-pressure, making them priority candidates for additional support coverage.
SELECT
    partner_name,
    fund_name,
    scheduled_close_date,
    total_committed_aum,
    tickets_0_to_7_days_before_close
FROM fact_partner_close_pressure
WHERE is_high_risk_high_value_close = TRUE
ORDER BY
    tickets_0_to_7_days_before_close DESC,
    total_committed_aum DESC;