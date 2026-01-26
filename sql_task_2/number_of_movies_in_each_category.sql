SELECT category.name, COUNT(film_id) AS film_count
FROM category
LEFT JOIN film_category ON film_category.category_id = category.category_id
GROUP BY category.name
ORDER BY COUNT(film_id) DESC