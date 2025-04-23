// nosql/mongodb_schema.js

db.createCollection("users", {
    validator: {
       $jsonSchema: {
          bsonType: "object",
          required: ["username", "email"],
          properties: {
             username: {
                bsonType: "string",
                description: "must be a string and is required"
             },
             email: {
                bsonType: "string",
                pattern: "^.+@.+$",
                description: "must be a valid email and is required"
             },
             created_at: {
                bsonType: "date",
                description: "date when the user was created"
             },
             last_login: {
                bsonType: "date",
                description: "date of last user login"
             },
             followers: {
                bsonType: "array",
                items: {
                   bsonType: "objectId",
                   description: "reference to user ids that follow this user"
                }
             },
             following: {
                bsonType: "array",
                items: {
                   bsonType: "objectId",
                   description: "reference to user ids that this user follows"
                }
             }
          }
       }
    }
 });
 
 db.createCollection("posts", {
    validator: {
       $jsonSchema: {
          bsonType: "object",
          required: ["user_id", "content"],
          properties: {
             user_id: {
                bsonType: "objectId",
                description: "must be an objectId and is required"
             },
             content: {
                bsonType: "string",
                description: "must be a string and is required"
             },
             created_at: {
                bsonType: "date",
                description: "date when the post was created"
             },
             is_public: {
                bsonType: "bool",
                description: "indicates if the post is public"
             },
             comments: {
                bsonType: "array",
                items: {
                   bsonType: "object",
                   required: ["user_id", "content"],
                   properties: {
                      user_id: { bsonType: "objectId" },
                      content: { bsonType: "string" },
                      created_at: { bsonType: "date" }
                   }
                }
             }
          }
       }
    }
 });
 
db.users.createIndex({ username: 1 }, { unique: true });
db.users.createIndex({ email: 1 }, { unique: true });
db.posts.createIndex({ user_id: 1 });
db.posts.createIndex({ created_at: -1 });