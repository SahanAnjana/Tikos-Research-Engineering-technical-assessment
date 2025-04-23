// graph/neo4j_schema.cypher

CREATE (u1:User {
    user_id: 1,
    username: "user1",
    email: "user1@example.com",
    created_at: datetime()
});

CREATE (p1:Post {
    post_id: 1,
    content: "This is my first post",
    created_at: datetime(),
    is_public: true
});

CREATE (c1:Comment {
    comment_id: 1,
    content: "Great post!",
    created_at: datetime()
});

MATCH (u:User {user_id: 1}), (p:Post {post_id: 1})
CREATE (u)-[:POSTED]->(p);

MATCH (u:User {user_id: 2}), (p:Post {post_id: 1})
CREATE (u)-[:COMMENTED]->(p);

MATCH (u1:User {user_id: 1}), (u2:User {user_id: 2})
CREATE (u1)-[:FOLLOWS]->(u2);

CREATE INDEX user_id_index FOR (u:User) ON (u.user_id);
CREATE INDEX post_id_index FOR (p:Post) ON (p.post_id);
CREATE INDEX user_username_index FOR (u:User) ON (u.username);