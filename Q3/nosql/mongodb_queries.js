// nosql/mongodb_queries.js

// Get user feed (posts from followed users)
db.posts.find({
    user_id: { $in: db.users.findOne({ _id: ObjectId("123") }).following },
    is_public: true
}).sort({ created_at: -1 }).limit(20);

// Get posts with comment counts
db.posts.aggregate([
    { $match: { user_id: ObjectId("123") } },
    { $project: {
        _id: 1,
        user_id: 1,
        content: 1,
        created_at: 1,
        comment_count: { $size: { $ifNull: ["$comments", []] } }
    }},
    { $sort: { created_at: -1 } }
]);

// Find mutual followers
db.users.aggregate([
    { $match: { _id: ObjectId("123") } },
    { $project: { following: 1 } },
    { $lookup: {
        from: "users",
        let: { user_following: "$following" },
        pipeline: [
            { $match: { _id: ObjectId("456") } },
            { $project: { 
                mutual_friends: { 
                    $setIntersection: ["$$user_following", "$following"] 
                } 
            }}
        ],
        as: "mutual_data"
    }},
    { $unwind: "$mutual_data" },
    { $project: { mutual_friends: "$mutual_data.mutual_friends" } }
]);