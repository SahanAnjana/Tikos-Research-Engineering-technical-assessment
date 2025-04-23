// graph/neo4j_schema.cypher

// Get user feed (posts from followed users)
MATCH (u:User {user_id: 123})-[:FOLLOWS]->(followed:User)-[:POSTED]->(p:Post)
WHERE p.is_public = true
RETURN p
ORDER BY p.created_at DESC
LIMIT 20;

// Get posts with comment counts
MATCH (u:User {user_id: 123})-[:POSTED]->(p:Post)
OPTIONAL MATCH (p)<-[c:COMMENTED]-()
RETURN p, count(c) AS comment_count
ORDER BY p.created_at DESC;

// Find mutual friends (users that both u1 and u2 follow)
MATCH (u1:User {user_id: 123})-[:FOLLOWS]->(mutual:User)<-[:FOLLOWS]-(u2:User {user_id: 456})
RETURN mutual;

// Find friend recommendations (friends of friends not already followed)
MATCH (u:User {user_id: 123})-[:FOLLOWS]->(:User)-[:FOLLOWS]->(potential:User)
WHERE NOT (u)-[:FOLLOWS]->(potential) AND u <> potential
RETURN potential, count(*) AS common_connections
ORDER BY common_connections DESC;