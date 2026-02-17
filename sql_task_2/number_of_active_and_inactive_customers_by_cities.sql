SELECT
city.city,
SUM(CASE WHEN customer.active = 1 THEN 1 ELSE 0 END) AS active_customers,
SUM(CASE WHEN customer.active = 0 THEN 1 ELSE 0 END) AS inactive_customers
FROM city
LEFT JOIN address ON address.city_id = city.city_id
LEFT JOIN customer ON customer.address_id = address.address_id
GROUP BY city.city
ORDER BY inactive_customers DESC