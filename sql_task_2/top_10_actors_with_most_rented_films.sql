WITH actor_ranks AS (
    SELECT
        a.first_name,
        a.last_name,
        COUNT(r.rental_id) AS rentals_count,
        DENSE_RANK() OVER (ORDER BY COUNT(r.rental_id) DESC) as rank
    FROM actor a
    LEFT JOIN film_actor fa ON fa.actor_id = a.actor_id
    LEFT JOIN film f ON f.film_id = fa.film_id
    LEFT JOIN inventory i ON i.film_id = f.film_id
    LEFT JOIN rental r ON r.inventory_id = i.inventory_id
    GROUP BY a.actor_id, a.first_name, a.last_name
)
SELECT first_name, last_name, rentals_count
FROM actor_ranks
WHERE rank <= 10;