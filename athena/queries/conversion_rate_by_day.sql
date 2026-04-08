SELECT
    event_date,
    COUNT(*) AS total_carts,
    SUM(CASE WHEN abandoned_cart_flag = 0 THEN 1 ELSE 0 END) AS converted_carts,
    SUM(CASE WHEN abandoned_cart_flag = 1 THEN 1 ELSE 0 END) AS abandoned_carts,
    100.0 * SUM(CASE WHEN abandoned_cart_flag = 0 THEN 1 ELSE 0 END) / COUNT(*) AS conversion_rate_pct
FROM abandoned_carts
GROUP BY event_date
ORDER BY event_date;