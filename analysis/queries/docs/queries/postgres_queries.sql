-- 1. How many products have an average review rating below 2.5?
SELECT COUNT(product_id)
FROM (
    SELECT product_id, AVG(rating) AS avg_rating
    FROM reviews
    GROUP BY product_id
    HAVING AVG(rating) < 2.5
) subquery;

-- 2. How many reviewers have written at least 3 reviews in the last year?
SELECT COUNT(reviewer_id)
FROM (
    SELECT reviewer_id, COUNT(*) AS review_count
    FROM reviews
    WHERE review_date >= NOW() - INTERVAL '1 year'
    GROUP BY reviewer_id
    HAVING COUNT(*) >= 3
) subquery;

-- 3. What is the average number of reviews with a 4.5-star rating per product category?
SELECT category, AVG(review_count) AS avg_reviews
FROM (
    SELECT category, product_id, COUNT(*) AS review_count
    FROM reviews r
    JOIN products p ON r.product_id = p.product_id
    WHERE rating = 4.5
    GROUP BY category, product_id
) subquery
GROUP BY category;
