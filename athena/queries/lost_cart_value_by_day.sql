SELECT
    event_date,
    SUM(cart_value) AS lost_cart_value
FROM abandoned_carts
WHERE abandoned_cart_flag = 1
GROUP BY event_date
ORDER BY event_date;