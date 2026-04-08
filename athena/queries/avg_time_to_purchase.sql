SELECT
    event_date,
    AVG(time_to_purchase_minutes) AS avg_time_to_purchase_minutes
FROM abandoned_carts
WHERE abandoned_cart_flag = 0
GROUP BY event_date
ORDER BY event_date;