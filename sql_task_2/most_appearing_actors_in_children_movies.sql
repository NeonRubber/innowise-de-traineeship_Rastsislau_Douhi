SELECT
actor_id,
first_name,
last_name,
appearance_number,
appearance_number_rank
FROM(
	SELECT
		actor.actor_id,
		actor.first_name,
		actor.last_name,
		COUNT(DISTINCT film_actor.film_id) AS appearance_number,
		DENSE_RANK() OVER (ORDER BY COUNT(DISTINCT film_actor.film_id) DESC) AS appearance_number_rank
	FROM actor
	LEFT JOIN film_actor ON film_actor.actor_id = actor.actor_id
	LEFT JOIN film ON film.film_id = film_actor.film_id
	LEFT JOIN film_category ON film_category.film_id = film.film_id
	LEFT JOIN category ON category.category_id = film_category.category_id
	WHERE category.name = 'Children'
	GROUP BY actor.actor_id, actor.first_name, actor.last_name
) subquery
WHERE appearance_number_rank <= 3
ORDER BY appearance_number DESC, first_name, last_name
