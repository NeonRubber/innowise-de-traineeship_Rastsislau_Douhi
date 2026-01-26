SELECT first_name, last_name, COUNT(rental.rental_id) AS rentals_count
FROM actor
LEFT JOIN film_actor ON film_actor.actor_id = actor.actor_id
LEFT JOIN film ON film.film_id = film_actor.film_id
LEFT JOIN inventory ON inventory.film_id = film.film_id
LEFT JOIN rental ON rental.inventory_id = inventory.inventory_id
GROUP BY actor.actor_id, first_name, last_name
ORDER BY rentals_count DESC LIMIT 10