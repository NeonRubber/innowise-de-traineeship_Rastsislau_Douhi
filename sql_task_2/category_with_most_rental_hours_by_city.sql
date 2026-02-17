WITH filtered_cities AS (
    SELECT city_id, city
    FROM city
    WHERE city LIKE 'A%' OR city LIKE '%-%'
),
city_rentals AS (
    SELECT
        fc.city,
        c.name AS category_name,
        SUM(EXTRACT(EPOCH FROM (r.return_date - r.rental_date)) / 3600) AS category_rental_hours
    FROM filtered_cities fc
    JOIN address a ON a.city_id = fc.city_id
    JOIN customer cu ON cu.address_id = a.address_id
    JOIN rental r ON r.customer_id = cu.customer_id
    JOIN inventory i ON i.inventory_id = r.inventory_id
    JOIN film f ON f.film_id = i.film_id
    JOIN film_category fcat ON fcat.film_id = f.film_id
    JOIN category c ON c.category_id = fcat.category_id
    GROUP BY fc.city, c.name
),
ranked_rentals AS (
    SELECT
        city,
        category_name,
        category_rental_hours,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY category_rental_hours DESC) AS rn
    FROM city_rentals
)
SELECT city, category_name, category_rental_hours
FROM ranked_rentals
WHERE rn = 1
ORDER BY city;