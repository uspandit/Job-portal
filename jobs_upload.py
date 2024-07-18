from pymongo import MongoClient
from bson import ObjectId
import json

# Function to load data from json file to MongoDB
def load_data_to_mongodb(json_file, db_name, collection_name):
    # Connecting to MongoDB
    connection_string = "mongodb://localhost:27017"
    client = MongoClient(connection_string)
    db = client[db_name]
    collection = db[collection_name]

    # Loading data from JSON file
    with open(json_file, 'r') as file:
        data = json.load(file)

    # Parsing ObjectId
    for item in data:
        if '_id' in item and '$oid' in item['_id']:
            item['_id'] = ObjectId(item['_id']['$oid'])

    # Inserting data into the collection
    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)

    print(f"Data loaded into {db_name}.{collection_name}")

if __name__ == "__main__":
    json_file = 'jobs_data.json'
    db_name = 'jobs'
    collection_name = 'narkuri_tech_jobs'
    
    load_data_to_mongodb(json_file, db_name, collection_name)