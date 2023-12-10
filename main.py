from pymongo import MongoClient

# MongoDB connection string
conn_string = "mongodb+srv://ds2002:UVA1819@cluster0.xuibg2h.mongodb.net/"

# Create a MongoClient
client = MongoClient(conn_string)

# Connect to the sample_mflix database
db = client.sample_mflix

# General Queries on Movies Collection
print("\n--- General Queries on Movies Collection ---")
# Total number of documents in the collection movies
movies_count = db.movies.count_documents({})
print("\nTotal number of movies:", movies_count)

# Display any 5 documents in pretty format
print("\nFirst 5 movies:")
for movie in db.movies.find().limit(5):
    print(movie)

# Display 5 documents sorted by “title”
print("\n5 Movies sorted by title:")
for movie in db.movies.find().sort("title", 1).limit(5):
    print(movie)

# Display 5 documents (only title and awards) sorted by “title”
print("\n5 Movies (title & awards) sorted by title:")
for movie in db.movies.find({}, {"title": 1, "awards": 1}).sort("title", 1).limit(5):
    print(movie)

# Display 5 documents (only title and awards) sorted by “title” in descending order
print("\n5 Movies (title & awards) sorted by title in descending order:")
for movie in db.movies.find({}, {"title": 1, "awards": 1}).sort("title", -1).limit(5):
    print(movie)

# Movies with most awards
print("\nMovies with most awards:")
for movie in db.movies.find({}, {"title": 1, "awards": 1}).sort("awards.wins", -1).limit(5):
    print(movie)

# Movie that won most awards
print("\nMovie that won most awards:")
top_award_movie = db.movies.find_one({}, {"title": 1, "awards": 1}, sort=[("awards.wins", -1)])
print(top_award_movie)

# $AND/$ALL Operation Queries
print("\n--- $AND/$ALL Operation Queries ---")
# Movies with genres “Adventure” and “Mystery”
print("\nMovies with genres Adventure and Mystery:")
for movie in db.movies.find({"genres": {"$all": ["Adventure", "Mystery"]}}).limit(5):
    print(movie)

# Movies with genre “Adventure” and cast “Tom Hanks”
print("\nMovies with genre Adventure and cast Tom Hanks:")
for movie in db.movies.find({"genres": "Adventure", "cast": "Tom Hanks"}).limit(5):
    print(movie)

# Aggregation Queries
print("\n--- Aggregation Queries ---")
# Average number of awards won by a movie
print("\nAverage number of awards won by a movie:")
avg_awards = db.movies.aggregate([
    {"$group": {"_id": None, "averageAwards": {"$avg": "$awards.wins"}}}
])
for result in avg_awards:
    print(result)

# Most awards won by a movie
print("\nMost awards won by a movie:")
max_awards = db.movies.aggregate([
    {"$group": {"_id": None, "maxAwards": {"$max": "$awards.wins"}}}
])
for result in max_awards:
    print(result)

# General Queries on Comments Collection
print("\n--- General Queries on Comments Collection ---")
# Total number of documents in the collection comments
comments_count = db.comments.count_documents({})
print("\nTotal number of comments:", comments_count)

# Total number of distinct users
distinct_users = len(db.comments.distinct("name"))
print("\nTotal number of distinct users:", distinct_users)

# Display any 5 comments in pretty format
print("\nFirst 5 comments:")
for comment in db.comments.find().limit(5):
    print(comment)

# Display 5 comments sorted by name
print("\n5 Comments sorted by name:")
for comment in db.comments.find().sort("name", 1).limit(5):
    print(comment)

# Display 5 latest comments from "Megan Richards"
print("\n5 latest comments from Megan Richards:")
for comment in db.comments.find({"name": "Megan Richards"}).sort("date", -1).limit(5):
    print(comment)
