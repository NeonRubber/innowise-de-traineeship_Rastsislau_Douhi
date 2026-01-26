SELECT city,
       category_name,
       category_rental_hours
FROM (
    SELECT
        city.city AS city,
        category.name AS category_name,
        SUM(EXTRACT(EPOCH FROM (rental.return_date - rental.rental_date)) / 3600) AS category_rental_hours,
        ROW_NUMBER() OVER (PARTITION BY city.city ORDER BY SUM(EXTRACT(EPOCH FROM (rental.return_date - rental.rental_date)) / 3600) DESC NULLS LAST) AS row_number
    FROM city
    LEFT JOIN address        ON address.city_id = city.city_id
    LEFT JOIN customer       ON customer.address_id = address.address_id
    LEFT JOIN rental         ON rental.customer_id = customer.customer_id
    LEFT JOIN inventory      ON inventory.inventory_id = rental.inventory_id
    LEFT JOIN film           ON film.film_id = inventory.film_id
    LEFT JOIN film_category  ON film_category.film_id = film.film_id
    LEFT JOIN category       ON category.category_id = film_category.category_id
    WHERE city.city LIKE 'A%' OR city.city LIKE '%-%'
    GROUP BY city.city, category.name
) hours_ranked
WHERE row_number = 1
ORDER BY city;