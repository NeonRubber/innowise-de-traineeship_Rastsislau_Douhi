SELECT film.film_id, title
FROM film
LEFT JOIN inventory ON inventory.film_id = film.film_id
WHERE inventory_id IS NULL
ORDER BY film_id ASC