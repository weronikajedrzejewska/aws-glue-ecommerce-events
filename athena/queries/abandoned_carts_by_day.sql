SELECT
    event_date,
    COUNT(*) AS abandoned_carts_count
FROM abandoned_carts
WHERE abandoned_cart_flag = 1
GROUP BY event_date
ORDER BY event_date;