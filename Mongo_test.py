import pymongo


def main():
    # STEPS:
    # connects to mongodb by using the user credentials of a user made in the corresponding database
    # moviesdb user credentials
    # user: moviemanager
    # pwd: movies456
    # established connection using its public ipv4 address

    # client = pymongo.MongoClient("mongodb://test:test123@54.226.137.30:27019/?directConnection=true&appName=mongosh+1.3.1&replicaSet=crs")
    # client = Connection("ip-172-31-27-170:27019")

    try:
        # establishing remote connection to aws mongos
        client = pymongo.MongoClient(
            "mongodb://moviemanager:movies456@54.172.89.9:27017/moviesdb?directConnection=true"
            "&serverSelectionTimeoutMS=2000&appName=mongosh+1.3.1")

        # confirms connection
        print("Connection established")
        # print(client.server_info())

        # pointers to the database and collections
        db = client["moviesdb"]
        collection = db["moviedata"]

        # print(collection.count_documents())
        # print(db)
        # print(collection)

        # runs ui infinitely until user wants to quit
        while 1:
            in_progress = ui(collection)
            if in_progress:
                break

        print("Connection Closing")
        client.close()
        print("Have a Nice Day!")

    except Exception as e:
        print(e)


def query_1(collection):
    title = input("Enter a Movie Title\n")

    most = collection.aggregate([{"$match": {"title": title} },{"$count": "rating"}])
    print(f"Number of Users Ratings for {title}:")

    for doc in most:
        print(doc)


def query_2(collection):
    queries = collection.find({})
    for doc in q:
        print(doc)
    pass


def query_3(collection):
    pass

# The user can enter a tag (or tags), specify if they want all of the tags to match or one of the tags to match,
# and the program will return a list of movies with all of those user tags or one of those tags, respectively.
def query_4(collection):
    print("Input a partial or complete actor name")
    actorString = input()
    print("Input a partial or complete movie title")
    titleString = input()

    results = collection.find({'$and': [{"starring": {'$regex': actorString}}, {"starring": {'$regex': titleString}}]})
    count = 0
    for doc in results:
        if count == 10:
            break
        print(doc)
        count += 1



def query_5(collection):
    pass


def query_6(collection):
    tag = input("Enter a tag:\n")
    movies = collection.find({"tag": tag}, {"_id": 0, "title": 1})

    print(f"Titles with the tag: {tag}")
    for doc in movies:
        print(doc)

# The user can enter a movie, and the program will return the average rating across all users who have rated it,
# along with any tags that have been applied by more than one user (or another filter on tags).
def query_7(collection):
    title = input("Enter a Movie Title:\n")
    average = collection.aggregate([{"$match": {"title": title}}, {"$group": {"_id": "$title", "rating": {"$avg": "$rating"}}}])

    for doc in average:
        print(doc)


# The user can enter whether they want a list of the best movies or the worst movies,
# and the program will return all of the top-rated movies or all of the worst rated movies across the dataset,
# within a certain threshold

def query_8(collection):
    title = input("Enter 1: Best or 2: Worst rating:\n")
    if int(title) == 1:

        best = collection.aggregate([{"$group": {"_id": "$title", "rating": {"$max": "$rating"}}}])
        print("Movies with Best Ratings:")

        for doc in best:
            print(doc)

    elif int(title) == 2:
        worst = collection.aggregate([{"$group": {"_id": "$title", "rating": {"$min": "$rating"}}}])
        print("Movies with Worst Ratings:")

        for doc in worst:
            print(doc)


# The user can enter their userID and return a list of movies they have rated
def query_9(collection):
    uid = input("Enter your User ID:\n")
    movies = collection.find({"user_id": int(uid)}, {"_id": 0, "title": 1})

    print(f"Titles rated by User: {uid}")
    for doc in movies:
        print(doc)


# The user can enter a movie title and return a list of tags associated with the respective movie
def query_10(collection):
    title = input("Enter a Movie Title:\n")
    tags = collection.find({"title": {"$regex": title}}, {"_id": 0, "tag": 1})

    print(f"Tags asscoiated with Movie: {title}")

    for doc in tags:
        if doc['tag'] != '':
            print(doc)


# User inputs the full title string of the movie to be updated, and the new title string.
# Function updates all movies with the specified title string, and changes the title to the new title string.
def query_11(collection):

    print("Input full title string of the movie to be updated")
    inputTargetTitle = input()

    print("Input new title string")
    inputUpdatedTitle = input()

    results = collection.update_many({"title": inputTargetTitle}, {"$set": {"title": inputUpdatedTitle}})
    print("Result of update query:\n")
    print("Raw result: ", results.raw_result)
    print("acknowledged: ", results.acknowledged)
    print("matched count:", results.matched_count)

# User inputs the full title string of a movie.
# The function deletes all movies with that title.
def query_12(collection):
    print("Input full title string of the movie to be deleted")
    inputString = input()

    results = collection.delete_many({'title': {"$eq": inputString}})
    print("Result of delete query:\n")
    print("Raw result: ", results.raw_result)
    print("acknowledged: ", results.acknowledged)
    print("matched count:", results.matched_count)

# User inputs a string.
# Function returns movies that contains the string in their title.
def query_13(collection):
    print("Input partial or full title string")
    inputString = input()

    results = collection.find({'title': {"$regex": inputString}})
    count = 0
    for doc in results:
        if count == 10:
            break
        print(doc)
        count += 1

# User inputs a lower bound rating and a higher bound rating.
# Function returns movies where the rating field is between the lower bound and the upper bound.
def query_14(collection):
    print("Input a lower bound rating (float)")
    lowerBound = float(input())
    print("Input a higher bound rating (float)")
    upperBound = float(input())

    results = collection.find({'rating': {"$lte": upperBound, "$gte": lowerBound}})
    count = 0
    for doc in results:
        if count == 10:
            break
        print(doc)
        count += 1

# User inputs an actor name, and this function searches for a movie that stars that actor.
def query_15(collection):

    print("Input an actor name")
    # saves user input
    inputString = input()
    results = collection.find({'starring': {"$regex": inputString}})
    count = 0
    for doc in results:
        if count == 10:
            break
        print(doc)
        count += 1


# user interface for management system
def ui(collection):
    print("Welcome to TeamCTJ Movie Management System!")
    print("Please select a number between 1-15")
    print("1. -")
    print("2. -")
    print("3. -")
    print("4. Similar User Favorites")
    print("5. -")
    print("6. Search By Tag")
    print("7. Average Movie Rating")
    print("8. Best/Worst Movies")
    print("9. Movies You Rated")
    print("10. Movie Tags")
    print("11. :Change title of movie")
    print("12. :Delete all movies of a title")
    print("13. :Search for movie by title")
    print("14. :Search for movie with rating range")
    print("15. :Search for Movie with Actor")

    # saves user input
    while 1:
        usernum = input("Enter a number:\n")
        usernum = int(usernum)

        if usernum == 1:
            query_1(collection)
            break
        elif usernum == 2:
            query_2(collection)
            break
        elif usernum == 3:
            query_3(collection)
            break
        elif usernum == 4:
            query_4(collection)
            break
        elif usernum == 5:
            query_5(collection)
            break
        elif usernum == 6:
            query_6(collection)
            break
        elif usernum == 7:
            query_7(collection)
            break
        elif usernum == 8:
            query_8(collection)
            break
        elif usernum == 9:
            query_9(collection)
            break
        elif usernum == 10:
            query_10(collection)
            break
        elif usernum == 11:
            query_11(collection)
            break
        elif usernum == 12:
            query_12(collection)
            break
        elif usernum == 13:
            query_13(collection)
            break
        elif usernum == 14:
            query_14(collection)
            break
        elif usernum == 15:
            query_15(collection)
            break
        else:
            print("Invalid Option")

    print()
    print("Query Complete!")

    print("Would you like to quit? Enter yes or no")

    _quit = input("Enter yes or no\n")

    if _quit == 'yes':
        return True
    else:
        return False


if __name__ == "__main__":
    main()
