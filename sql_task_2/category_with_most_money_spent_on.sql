WITH category_revenue AS (
    SELECT
        c.name,
        SUM(p.amount) AS money_spent,
        DENSE_RANK() OVER (ORDER BY SUM(p.amount) DESC) as rank
    FROM payment p
    JOIN rental r ON r.rental_id = p.rental_id
    JOIN inventory i ON i.inventory_id = r.inventory_id
    JOIN film f ON f.film_id = i.film_id
    JOIN film_category fc ON fc.film_id = f.film_id
    JOIN category c ON c.category_id = fc.category_id
    GROUP BY c.name
)
SELECT name, money_spent
FROM category_revenue
WHERE rank = 1;