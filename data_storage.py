from pymongo import MongoClient
from config import Config

def write_data(data):
    client = MongoClient(Config.MONGO_URI)
    db = client[Config.MONGO_DB]
    collection = db[Config.MONGO_COLLECTION]
    collection.delete_many({})
    collection.insert_many(data)

    print ("Data written to MongoDB successfully")

    client.close()