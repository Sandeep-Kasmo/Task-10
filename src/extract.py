import pandas as pd #type:ignore
from pymongo import MongoClient #type:ignore
from bson.objectid import ObjectId #type:ignore

# global client
client = None

def connect_mongodb():
    global client
    try:
        client = MongoClient("mongodb://localhost:27017/")
        print("MongoDB connection successful!")
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None


def get_data(db_name, collection_name, query={}):
    global client
    if client is None:
        connect_mongodb()

    db = client[db_name]
    collection = db[collection_name]

    try:
        data = list(collection.find(query))

        # If collection empty
        if not data:
            print("No documents found.")
            return pd.DataFrame()

        # Convert ObjectId → string
        for d in data:
            if "_id" in d:
                d["_id"] = str(d["_id"])

        dataframe = pd.DataFrame(data)
        return dataframe

    except Exception as e:
        print(f"Error reading collection {collection_name}: {e}")
        return pd.DataFrame()


def close_mongodb():
    global client
    if client:
        client.close()
        print("MongoDB connection closed!")
