-- sql/sql_queries.sql

-- Get all posts from users that a specific user follows
SELECT p.*
FROM posts p
JOIN user_relationships ur ON p.user_id = ur.user_id_2
WHERE ur.user_id_1 = 123 
  AND ur.relationship_type = 'follow'
  AND p.is_public = true
ORDER BY p.created_at DESC
LIMIT 20;

-- Get posts with comment counts
SELECT p.*, COUNT(c.comment_id) AS comment_count
FROM posts p
LEFT JOIN comments c ON p.post_id = c.post_id
WHERE p.user_id = 123
GROUP BY p.post_id
ORDER BY p.created_at DESC;

-- Find mutual friends
SELECT u.username
FROM users u
WHERE EXISTS (
    SELECT 1 FROM user_relationships ur1
    WHERE ur1.user_id_1 = 123 
      AND ur1.user_id_2 = u.user_id
      AND ur1.relationship_type = 'friend'
) AND EXISTS (
    SELECT 1 FROM user_relationships ur2
    WHERE ur2.user_id_1 = 456 
      AND ur2.user_id_2 = u.user_id
      AND ur2.relationship_type = 'friend'
);